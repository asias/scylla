/*
 * Copyright (C) 2019 pengjian.uestc @ gmail.com
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include "seastar/core/shared_ptr.hh"
#include "seastar/core/future.hh"
#include "bytes.hh"

using namespace seastar;

namespace service {
class storage_proxy;
class client_state;
}

class service_permit;

namespace redis {

class redis_options;

struct strings_result {
    bytes _result;
    bool _has_result;
    bytes& result() { return _result; }
    bool has_result() const { return _has_result; }
};

future<lw_shared_ptr<strings_result>> read_strings(service::storage_proxy&, const redis_options&, const bytes&, service_permit);

}
