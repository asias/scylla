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
 * Copyright (C) 2015 ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#include "cql3/update_parameters.hh"

namespace cql3 {

const update_parameters::prefetch_data::cell_list*
update_parameters::get_prefetched_list(
    partition_key_view pkey,
    clustering_key_view ckey,
    const column_definition& column) const
{
    if (!_prefetched) {
        return {};
    }

    auto i = _prefetched->rows.find(std::make_pair(std::move(pkey), std::move(ckey)));
    if (i == _prefetched->rows.end()) {
        return {};
    }

    auto&& row = i->second;
    auto j = row.find(column.id);
    if (j == row.end()) {
        return {};
    }
    return &j->second;
}

update_parameters::prefetch_data::prefetch_data(schema_ptr schema)
    : rows(8, key_hashing(*schema), key_equality(*schema))
    , schema(schema)
{ }

}
