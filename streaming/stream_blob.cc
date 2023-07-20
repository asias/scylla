/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "log.hh"
#include "streaming/stream_blob.hh"
#include "message/messaging_service.hh"
#include "gms/inet_address.hh"
#include <seastar/core/coroutine.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/future.hh>
#include <vector>
#include <seastar/core/fstream.hh>
#include <seastar/coroutine/parallel_for_each.hh>
#include <seastar/coroutine/all.hh>
#include "utils/pretty_printers.hh"
#include <cfloat>

namespace streaming {

static logging::logger blogger("stream_blob");

static utils::pretty_printed_throughput get_bw(size_t total_size, std::chrono::steady_clock::time_point start_time) {
    auto duration = std::chrono::duration<float>(std::chrono::steady_clock::now() - start_time);
    return utils::pretty_printed_throughput(total_size, duration);
}

sstring get_dest_file_name() {
    // TODO: we need to get a real path for the destination
    auto uuid = utils::make_random_uuid();
    auto filename = "/tmp/rx-" + uuid.to_sstring();
    return filename;
}

future<> stream_blob_handler(netw::messaging_service& ms,
        gms::inet_address from,
        streaming::stream_blob_meta meta,
        rpc::sink<streaming::stream_blob_cmd> sink,
        rpc::source<streaming::stream_blob_data, streaming::stream_blob_cmd> source) {
    bool fstream_closed = false;
    bool sink_closed = false;
    bool status_sent = false;
    std::optional<output_stream<char>> fstream;
    size_t total_size = 0;
    auto start_time = std::chrono::steady_clock::now();
    std::exception_ptr error;
    try {
        blogger.info("fstream[{}] Follower started peer={} file={}",
                meta.ops_id, from, meta.filename);
        auto file = co_await open_file_dma(get_dest_file_name(), open_flags::wo | open_flags::create);
        fstream = co_await make_file_output_stream(std::move(file));
        for (;;) {
            auto opt = co_await source();
            if (!opt) {
                break;
            }
            auto& cmd = std::get<1>(*opt);
            if (cmd == streaming::stream_blob_cmd::error) {
                blogger.warn("fstream[{}] Follower got stream_blob_cmd::error from peer={} file={}",
                        meta.ops_id, from, meta.filename);
                throw std::runtime_error(format("Got stream_blob_cmd::error from peer={} file={}", from, meta.filename));
            } else if (cmd == streaming::stream_blob_cmd::end_of_stream) {
                blogger.debug("fstream[{}] Follower got stream_blob_cmd::end_of_stream from peer={} file={}",
                        meta.ops_id, from, meta.filename);
            }
            streaming::stream_blob_data& data = std::get<0>(*opt);
            total_size += data.data.size();
            blogger.trace("fstream[{}] Follower received data from peer={} data={}", meta.ops_id, from, data.data.size());
            if (!data.data.empty()) {
                co_await fstream->write((char*)data.data.data(), data.data.size());
            }
        }
        co_await fstream->flush();
        co_await fstream->close();
        fstream_closed = true;

        // Send status code and close the sink
        co_await sink(streaming::stream_blob_cmd::ok);
        status_sent = true;
        co_await sink.close();
        sink_closed = true;
    } catch (...) {
        error = std::current_exception();
    }
    if (error) {
        blogger.warn("fstream[{}] Follower failed peer={} file={} received_size={} bw={} error={}",
                meta.ops_id, from, meta.filename, total_size, get_bw(total_size, start_time), error);
        if (!fstream_closed) {
            try {
                if (fstream) {
                    // Make sure fstream is always closed
                    co_await fstream->close();
                }
            } catch (...) {
                // We could do nothing but continue to cleanup more
            }
        }
        if (!status_sent) {
            try {
                co_await sink(streaming::stream_blob_cmd::error);
            } catch (...) {
                // Try our best to send the status code.
                // If we could not send it, we could do nothing but close the sink.
            }
        }
        if (!sink_closed) {
            // Make sure sink is always closed
            co_await sink.close();
        }
        // Do not call rethrow_exception(error) because the caller could do nothing but log
        // the error. We have already logged the error here.
    } else {
        // Get some statistics
        blogger.info("fstream[{}] Follower finished peer={} file={} received_size={} bw={}",
                meta.ops_id, from, meta.filename, total_size, get_bw(total_size, start_time));
    }
    co_return;
}

future<> stream_files(netw::messaging_service& ms, std::vector<seastar::sstring> files, std::vector<gms::inet_address> targets) {
    if (targets.empty()) {
        co_return;
    }
    if (files.empty()) {
        co_return;
    }
    auto uuid = utils::make_random_uuid();

    blogger.info("fstream[{}] Master started files={}, targets={}", uuid, files, targets);

    struct sink_and_source {
        gms::inet_address node;
        rpc::sink<streaming::stream_blob_data, streaming::stream_blob_cmd> sink;
        rpc::source<streaming::stream_blob_cmd> source;
        bool sink_closed = false;
        bool status_sent = false;
    };

    auto ops_start_time = std::chrono::steady_clock::now();
    size_t ops_total_size = 0;
    streaming::stream_blob_meta meta;
    meta.ops_id = uuid;
    std::exception_ptr error;

    for (auto& filename : files) {
        std::optional<input_stream<char>> fstream;
        bool fstream_closed = false;
        try {
            auto file = co_await open_file_dma(filename, open_flags::ro);
            fstream = make_file_input_stream(std::move(file));
        } catch (...) {
            blogger.info("fstream[{}] Master failed file={} targets={} error={}",
                uuid, files, targets, std::current_exception());
            throw;
        }

        std::vector<sink_and_source> ss;
        meta.filename = filename;
        size_t total_size = 0;
        auto start_time = std::chrono::steady_clock::now();
        bool got_error_from_peer = false;
        try {
            for (auto& node : targets) {
                blogger.debug("fstream[{}] Master creating sink and source for node={}, file={}, targets={}", uuid, node, filename, targets);
                auto [sink, source] = co_await ms.make_sink_and_source_for_stream_blob(meta, netw::messaging_service::msg_addr(node));
                ss.push_back(sink_and_source{node, std::move(sink), std::move(source)});
            }

            // This filer sends data to peer node
            auto send_data_to_peer = [&] () mutable -> future<> {
                const size_t batch_size = 64 * 1024;
                while (!got_error_from_peer) {
                    auto buf = co_await fstream->read_up_to(batch_size);
                    if (buf.size() == 0) {
                        break;
                    }
                    streaming::stream_blob_data data;
                    data.data.insert(data.data.end(), buf.begin(), buf.end());
                    co_await coroutine::parallel_for_each(ss, [&] (sink_and_source& s) mutable -> future<> {
                        auto sz = data.data.size();
                        total_size += sz;
                        ops_total_size += sz;
                        blogger.trace("fstream[{}] Master sending file={} to node={} chunk_size={}",
                            uuid, filename, s.node, data.data.size());
                        co_await s.sink(data, streaming::stream_blob_cmd::data);
                    });
                }

                if (fstream) {
                    co_await fstream->close();
                    fstream_closed = true;
                }

                for (auto& s : ss) {
                    blogger.debug("fstream[{}] Master done sending file={} to node={}", uuid, filename, s.node);
                    co_await s.sink(streaming::stream_blob_data{}, streaming::stream_blob_cmd::end_of_stream);
                    s.status_sent = true;
                    co_await s.sink.close();
                    s.sink_closed = true;
                }
                co_return;
            };

            // This fiber gets status code from peer node
            auto get_status_code_from_peer = [&] () mutable -> future<> {
                co_await coroutine::parallel_for_each(ss, [&] (sink_and_source& s) mutable -> future<> {
                    while (!got_error_from_peer) {
                        auto status_opt = co_await s.source();
                        if (status_opt) {
                            auto status = std::get<0>(*status_opt);
                            if (status == streaming::stream_blob_cmd::error) {
                                got_error_from_peer = true;
                            }
                            blogger.debug("fstream[{}] Master got stream_blob_cmd={} file={} peer={}",
                                    uuid, int(status), filename, s.node);
                        } else {
                            break;
                        }
                    }
                });
                co_return;
            };

            co_await coroutine::all(send_data_to_peer, get_status_code_from_peer);
        } catch (...) {
            error = std::current_exception();
        }
        if (error) {
            blogger.warn("fstream[{}] Master failed sending file={} to targets={} send_size={} bw={} error={}",
                    uuid, filename, targets, total_size, get_bw(total_size, start_time), error);
            // Error handling for fstream and sink
            if (!fstream_closed) {
                try {
                    if (fstream) {
                        co_await fstream->close();
                    }
                } catch (...) {
                    // We could do nothing but continue to cleanup more
                }
            }
            for (auto& s : ss) {
                if (s.status_sent && s.sink_closed) {
                    // We are done with the node
                    continue;
                }
                try {
                    if (!s.status_sent) {
                        co_await s.sink(streaming::stream_blob_data{}, streaming::stream_blob_cmd::error);
                        s.status_sent = true;
                    }
                } catch (...) {
                    // We could do nothing but continue to close as much as possible
                }
                try {
                    if (!s.sink_closed) {
                        co_await s.sink.close();
                        s.sink_closed = true;
                    }
                } catch (...) {
                    // We could do nothing but continue to close as much as possible
                }
            }
            // Stop handling remaining files
            break;
        } else {
            blogger.info("fstream[{}] Master done sending file={} to targets={} send_size={} bw={}",
                    uuid, filename, targets, total_size, get_bw(total_size, start_time));
        }
    }
    if (error) {
        blogger.info("fstream[{}] Master failed files={} targets={} send_size={} bw={} error={}",
                uuid, files, targets, ops_total_size, get_bw(ops_total_size, ops_start_time), error);
        std::rethrow_exception(error);
    } else {
        blogger.info("fstream[{}] Master finished files={} targets={} send_size={} bw={}",
                uuid, files, targets, ops_total_size, get_bw(ops_total_size, ops_start_time));
    }
    co_return;
}

}
