/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Copyright (C) 2014 ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include "service/client_state.hh"
#include "service/query_state.hh"
#include "service/storage_proxy.hh"
#include "cql3/query_options.hh"
#include "timeout_config.hh"
#include "audit/audit.hh"

namespace cql_transport {

namespace messages {

class result_message;

}

}

namespace cql3 {

class metadata;
shared_ptr<const metadata> make_empty_metadata();

class cql_statement {
    timeout_config_selector _timeout_config_selector;
    audit::audit_info_ptr _audit_info;
public:
    explicit cql_statement(timeout_config_selector timeout_selector) : _timeout_config_selector(timeout_selector) {}
    cql_statement(cql_statement&& o) = default;
    cql_statement(const cql_statement& o) : _timeout_config_selector(o._timeout_config_selector), _audit_info(o._audit_info ? std::make_unique<audit::audit_info>(*o._audit_info) : nullptr) { }
    virtual ~cql_statement()
    { }

    timeout_config_selector get_timeout_config_selector() const { return _timeout_config_selector; }

    virtual uint32_t get_bound_terms() const = 0;

    /**
     * Perform any access verification necessary for the statement.
     *
     * @param state the current client state
     */
    virtual future<> check_access(const service::client_state& state) const = 0;

    /**
     * Perform additional validation required by the statment.
     * To be overriden by subclasses if needed.
     *
     * @param state the current client state
     */
    virtual void validate(service::storage_proxy& proxy, const service::client_state& state) const = 0;

    /**
     * Execute the statement and return the resulting result or null if there is no result.
     *
     * @param state the current query state
     * @param options options for this query (consistency, variables, pageSize, ...)
     */
    virtual future<::shared_ptr<cql_transport::messages::result_message>>
        execute(service::storage_proxy& proxy, service::query_state& state, const query_options& options) const = 0;

    virtual bool uses_function(const sstring& ks_name, const sstring& function_name) const = 0;

    virtual bool depends_on_keyspace(const sstring& ks_name) const = 0;

    virtual bool depends_on_column_family(const sstring& cf_name) const = 0;

    virtual shared_ptr<const metadata> get_result_metadata() const = 0;

    audit::audit_info* get_audit_info() { return _audit_info.get(); }
    void set_audit_info(audit::audit_info_ptr&& info) { _audit_info = std::move(info); }

    virtual void sanitize_audit_info() {}
};

class cql_statement_no_metadata : public cql_statement {
public:
    using cql_statement::cql_statement;
    virtual shared_ptr<const metadata> get_result_metadata() const override {
        return make_empty_metadata();
    }
};

// Conditional modification statements and batches
// return a result set and have metadata, while same
// statements without conditions do not.
class cql_statement_opt_metadata : public cql_statement {
protected:
    // Result set metadata, may be empty for simple updates and batches
    shared_ptr<metadata> _metadata;
public:
    using cql_statement::cql_statement;
    virtual shared_ptr<const metadata> get_result_metadata() const override {
        if (_metadata) {
            return _metadata;
        }
        return make_empty_metadata();
    }
};

}
