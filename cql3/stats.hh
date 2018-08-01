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

#pragma once

namespace cql3 {

struct cql_stats {
    uint64_t reads = 0;
    uint64_t inserts = 0;
    uint64_t updates = 0;
    uint64_t deletes = 0;
    uint64_t batches = 0;
    uint64_t statements_in_batches = 0;
    uint64_t batches_pure_logged = 0;
    uint64_t batches_pure_unlogged = 0;
    uint64_t batches_unlogged_from_logged = 0;
    uint64_t rows_read = 0;
    uint64_t reverse_queries = 0;

    int64_t secondary_index_creates = 0;
    int64_t secondary_index_drops = 0;
    int64_t secondary_index_reads = 0;
    int64_t secondary_index_rows_read = 0;

    int64_t filtered_reads = 0;
    int64_t filtered_rows_matched_total = 0;
    int64_t filtered_rows_read_total = 0;
};

}
