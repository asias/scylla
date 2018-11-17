/*
 * Copyright (C) 2018 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include <type_traits>

#include <seastar/util/bool_class.hh>
#include "bytes.hh"
#include "types.hh"
#include "timestamp.hh"

class schema;
class row;
class clustering_key_prefix;
class encoding_stats;

namespace sstables {

class file_writer;

using indexed_columns = std::vector<std::reference_wrapper<const column_definition>>;

// Utilities for writing integral values in variable-length format
// See vint-serialization.hh for more details
void write_unsigned_vint(file_writer& out, uint64_t value);
void write_signed_vint(file_writer& out, int64_t value);

template <typename T>
typename std::enable_if_t<!std::is_integral_v<T>>
write_vint(file_writer& out, T t) = delete;

template <typename T>
inline void write_vint(file_writer& out, T value) {
    static_assert(std::is_integral_v<T>, "Non-integral values can't be written using write_vint");
    return std::is_unsigned_v<T> ? write_unsigned_vint(out, value) : write_signed_vint(out, value);
}

// There is a special case when we need to treat a non-full clustering key prefix as a full one
// for serialization purposes. This is the case that may occur with a compact table.
// For historical reasons a compact table may have rows with missing trailing clustering columns in their clustering keys.
// Consider:
//    cqlsh:test> CREATE TABLE cf (pk int, ck1 int, ck2 int, rc int, primary key (pk, ck1, ck2)) WITH COMPACT STORAGE;
//    cqlsh:test> INSERT INTO cf (pk, ck1, rc) VALUES (1, 1, 1);
//    cqlsh:test> SELECT * FROM cf;
//
//     pk | ck1 | ck2  | rc
//    ----+-----+------+----
//      1 |   1 | null |  1
//
//    (1 rows)
// In this case, the clustering key of the row will have length 1, but for serialization purposes we want to treat
// it as a full prefix of length 2.
// So we use ephemerally_full_prefix to distinguish this kind of clustering keys
using ephemerally_full_prefix = seastar::bool_class<struct ephemerally_full_prefix_tag>;

// Writes clustering prefix, full or not, encoded in SSTables 3.0 format
void write_clustering_prefix(file_writer& out, const schema& s,
        const clustering_key_prefix& prefix, ephemerally_full_prefix is_ephemerally_full);

// Writes encoded information about missing columns in the given row
void write_missing_columns(file_writer& out, const indexed_columns& columns, const row& row);

// Helper functions for writing delta-encoded time-related values
void write_delta_timestamp(file_writer& out, api::timestamp_type timestamp, const encoding_stats& enc_stats);

void write_delta_ttl(file_writer& out, uint32_t ttl, const encoding_stats& enc_stats);

void write_delta_local_deletion_time(file_writer& out, uint32_t local_deletion_time, const encoding_stats& enc_stats);

void write_delta_deletion_time(file_writer& out, deletion_time dt, const encoding_stats& enc_stats);

};   // namespace sstables
