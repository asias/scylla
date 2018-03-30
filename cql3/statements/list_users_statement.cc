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
 * Copyright 2016 ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#include "list_users_statement.hh"
#include "cql3/query_processor.hh"
#include "cql3/query_options.hh"
#include "auth/common.hh"

void cql3::statements::list_users_statement::validate(distributed<service::storage_proxy>& proxy, const service::client_state& state) {
}

future<> cql3::statements::list_users_statement::check_access(const service::client_state& state) {
    state.ensure_not_anonymous();
    return make_ready_future();
}

future<::shared_ptr<cql_transport::messages::result_message>>
cql3::statements::list_users_statement::execute(distributed<service::storage_proxy>& proxy, service::query_state& state, const query_options& options) {
    static const sstring query = sprint("SELECT * FROM %s.%s", auth::meta::AUTH_KS, auth::meta::USERS_CF);
    auto& qp = get_local_query_processor();
    return qp.process_internal(qp.prepare_internal(query), db::consistency_level::QUORUM, {});
}
