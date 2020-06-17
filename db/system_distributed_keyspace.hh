/*
 * Copyright (C) 2018 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include "bytes.hh"
#include "schema_fwd.hh"
#include "service/migration_manager.hh"
#include "service/qos/qos_common.hh"
#include "utils/UUID.hh"

#include <seastar/core/future.hh>
#include <seastar/core/sstring.hh>

#include <unordered_map>

namespace cql3 {
class query_processor;
}

namespace cdc {
    class stream_id;
    class topology_description;
} // namespace cdc

namespace db {

class system_distributed_keyspace {
public:
    static constexpr auto NAME = "system_distributed";
    static constexpr auto VIEW_BUILD_STATUS = "view_build_status";
    static constexpr auto SERVICE_LEVELS = "service_levels";

    /* Nodes use this table to communicate new CDC stream generations to other nodes. */
    static constexpr auto CDC_TOPOLOGY_DESCRIPTION = "cdc_topology_description";

    /* This table is used by CDC clients to learn about avaliable CDC streams. */
    static constexpr auto CDC_DESC = "cdc_description";

    /* Information required to modify/query some system_distributed tables, passed from the caller. */
    struct context {
        /* How many different token owners (endpoints) are there in the token ring? */
        size_t num_token_owners;
    };
private:
    cql3::query_processor& _qp;
    service::migration_manager& _mm;

public:
    system_distributed_keyspace(cql3::query_processor&, service::migration_manager&);

    future<> start();
    future<> stop();

    future<std::unordered_map<utils::UUID, sstring>> view_status(sstring ks_name, sstring view_name) const;
    future<> start_view_build(sstring ks_name, sstring view_name) const;
    future<> finish_view_build(sstring ks_name, sstring view_name) const;
    future<> remove_view(sstring ks_name, sstring view_name) const;

    future<> insert_cdc_topology_description(db_clock::time_point streams_ts, const cdc::topology_description&, context);
    future<std::optional<cdc::topology_description>> read_cdc_topology_description(db_clock::time_point streams_ts, context);
    future<> expire_cdc_topology_description(db_clock::time_point streams_ts, db_clock::time_point expiration_time, context);

    future<> create_cdc_desc(db_clock::time_point streams_ts, const std::vector<cdc::stream_id>&, context);
    future<> expire_cdc_desc(db_clock::time_point streams_ts, db_clock::time_point expiration_time, context);
    future<bool> cdc_desc_exists(db_clock::time_point streams_ts, context);

    future<qos::service_levels_info> get_service_levels() const;
    future<qos::service_levels_info> get_service_level(sstring service_level_name) const;
    future<> set_service_level(sstring service_level_name, qos::service_level_options slo) const;
    future<> drop_service_level(sstring service_level_name) const;
};

}
