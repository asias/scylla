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
#include "replica/database.hh"
#include "sstables/sstables.hh"
#include "sstables/sstables_manager.hh"
#include "sstables/sstable_version.hh"
#include <filesystem>


namespace streaming {

static logging::logger blogger("stream_blob");

static utils::pretty_printed_throughput get_bw(size_t total_size, std::chrono::steady_clock::time_point start_time) {
    auto duration = std::chrono::duration<float>(std::chrono::steady_clock::now() - start_time);
    return utils::pretty_printed_throughput(total_size, duration);
}

sstring get_dest_file_name(replica::database& db, const streaming::stream_blob_meta& meta) {
    auto path = std::filesystem::path(meta.filename);
    auto& table = db.find_column_family(meta.table);
    auto filename = std::filesystem::path(table.dir()) / path.filename();
    blogger.info("table_dir={} filename={} ret={}", table.dir(), path.filename(), filename);
    return filename.string();
}

future<> stream_blob_handler(replica::database& db, netw::messaging_service& ms,
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
        auto dst_filename = get_dest_file_name(db, meta);
        auto file = co_await open_file_dma(dst_filename, open_flags::wo | open_flags::create);
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

        // The sender sends TOC file in the end of the sstable component files.
        // When TOC file is received, the other sstable component files should
        // have been recevied. Load the received sstable to the main sstable
        // dataset.
        auto toc = sstables::sstable_version_constants::TOC_SUFFIX;
        auto data = sstring("Data.db");
        auto it = dst_filename.find(toc);
        if (it != sstring::npos) {
            auto data_filename = dst_filename;
            //data_filename.replace(it, toc.size(), data.c_str(), data.size());
            auto data_path = std::filesystem::path(data_filename).filename();
            blogger.info("fstream[{}] Started loading sst {}", meta.ops_id, data_filename);
            auto& table = db.find_column_family(meta.table);
            auto desc = sstables::entry_descriptor::make_descriptor(table.dir(), data_path.string(), table.schema()->ks_name(), table.schema()->cf_name());
#if 0
            co_await table.load_sstable_and_update_cache(desc);
#else
            co_await replica::database::load_sstable_for_tablet(db.container(), table.schema(), desc);
#endif
            blogger.info("fstream[{}] Finished loading sst {}", meta.ops_id, data_filename);
        }

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

future<> stream_files(netw::messaging_service& ms, std::list<seastar::sstring> files, std::vector<gms::inet_address> targets, table_id table, utils::UUID ops_id) {
    if (targets.empty()) {
        co_return;
    }
    if (files.empty()) {
        co_return;
    }

    blogger.info("fstream[{}] Master started sending files_nr={}, files={}, targets={}",
            ops_id, files.size(), files, targets);

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
    meta.ops_id = ops_id;
    meta.table = table;
    std::exception_ptr error;

    for (auto& filename : files) {
        std::optional<input_stream<char>> fstream;
        bool fstream_closed = false;
        try {
            auto file = co_await open_file_dma(filename, open_flags::ro);
            fstream = make_file_input_stream(std::move(file));
        } catch (...) {
            blogger.info("fstream[{}] Master failed file={} targets={} error={}",
                ops_id, files, targets, std::current_exception());
            throw;
        }

        std::vector<sink_and_source> ss;
        meta.filename = filename;
        size_t total_size = 0;
        auto start_time = std::chrono::steady_clock::now();
        bool got_error_from_peer = false;
        try {
            for (auto& node : targets) {
                blogger.debug("fstream[{}] Master creating sink and source for node={}, file={}, targets={}", ops_id, node, filename, targets);
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
                            ops_id, filename, s.node, data.data.size());
                        co_await s.sink(data, streaming::stream_blob_cmd::data);
                    });
                }

                if (fstream) {
                    co_await fstream->close();
                    fstream_closed = true;
                }

                for (auto& s : ss) {
                    blogger.debug("fstream[{}] Master done sending file={} to node={}", ops_id, filename, s.node);
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
                                    ops_id, int(status), filename, s.node);
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
                    ops_id, filename, targets, total_size, get_bw(total_size, start_time), error);
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
                    ops_id, filename, targets, total_size, get_bw(total_size, start_time));
        }
    }
    if (error) {
        blogger.info("fstream[{}] Master failed sending files_nr={}, files={} targets={} send_size={} bw={} error={}",
                ops_id, files.size(), files, targets, ops_total_size, get_bw(ops_total_size, ops_start_time), error);
        std::rethrow_exception(error);
    } else {
        blogger.info("fstream[{}] Master finished sending files_nr={}, files={} targets={} send_size={} bw={}",
                ops_id, files.size(), files, targets, ops_total_size, get_bw(ops_total_size, ops_start_time));
    }
    co_return;
}


future<> stream_sstables(replica::database& db, netw::messaging_service& ms, streaming::stream_files_request req) {
    auto& table = db.find_column_family(req.table);
    auto sstables = co_await table.take_storage_snapshot(req.range);
    auto files = std::list<sstring>();
    for (auto& sst : sstables) {
        auto components = std::list<sstring>();
        for (auto& c : sst->component_filenames()) {
            co_await coroutine::maybe_yield();
            // Put TOC file at the end of the file list for a given sstable
            if (c.find(sstables::sstable_version_constants::TOC_SUFFIX) != sstring::npos) {
                components.push_back(c);
            } else {
                components.push_front(c);
            }
        }
        for (auto& c : components) {
            files.push_back(c);
        }
    }
    blogger.info("stream_sstables[{}] Started sending sstable_nr={} files_nr={}, files={} range={}",
            req.ops_id, sstables.size(), files.size(), files, req.range);
    co_await stream_files(ms, files, req.targets, req.table, req.ops_id);
    blogger.info("stream_sstables[{}] Finished sending sstable_nr={} files_nr={}, files={} range={}",
            req.ops_id, sstables.size(), files.size(), files, req.range);
    co_return;
}

}
