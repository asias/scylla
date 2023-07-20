/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "message/messaging_service_fwd.hh"
#include <cstdint>
#include <vector>
#include <seastar/core/sstring.hh>
#include <seastar/core/future.hh>
#include <seastar/rpc/rpc_types.hh>
#include "utils/UUID.hh"

namespace streaming {

enum class stream_blob_cmd : uint8_t {
    ok,
    error,
    data,
    end_of_stream,
};

class stream_blob_data {
public:
    std::vector<uint8_t> data;
};

class stream_blob_meta {
public:
    utils::UUID ops_id;
    utils::UUID table_id;
    sstring filename;
};

// Send files in the files list to the nodes in targets list over network
seastar::future<> stream_files(netw::messaging_service& ms, std::vector<seastar::sstring> files, std::vector<gms::inet_address> targets);

seastar::future<> stream_blob_handler(netw::messaging_service& ms, gms::inet_address from, streaming::stream_blob_meta meta, rpc::sink<streaming::stream_blob_cmd> sink, rpc::source<streaming::stream_blob_data, streaming::stream_blob_cmd> source);

}
