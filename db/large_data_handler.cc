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

#include <seastar/core/print.hh>
#include "db/query_context.hh"
#include "db/system_keyspace.hh"
#include "db/large_data_handler.hh"
#include "sstables/sstables.hh"

namespace db {

future<> large_data_handler::maybe_update_large_partitions(const sstables::sstable& sst, const sstables::key& key, uint64_t partition_size) const {
    assert(!_stopped);
    if (partition_size > _partition_threshold_bytes) {
        ++_stats.partitions_bigger_than_threshold;

        const schema& s = *sst.get_schema();
        return update_large_partitions(s, sst.get_filename(), key, partition_size);
    }
    return make_ready_future<>();
}

logging::logger cql_table_large_data_handler::large_data_logger("large_data");

template <typename T> static std::string key_to_str(const T& key, const schema& s) {
    std::ostringstream oss;
    oss << key.with_schema(s);
    return oss.str();
}

future<> cql_table_large_data_handler::update_large_partitions(const schema& s, const sstring& sstable_name, const sstables::key& key, uint64_t partition_size) const {
    static const sstring req = format("INSERT INTO system.{} (keyspace_name, table_name, sstable_name, partition_size, partition_key, compaction_time) VALUES (?, ?, ?, ?, ?, ?) USING TTL 2592000",
            db::system_keyspace::LARGE_PARTITIONS);
    auto ks_name = s.ks_name();
    auto cf_name = s.cf_name();
    auto timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(db_clock::now().time_since_epoch()).count();
    auto key_str = key_to_str(key.to_partition_key(s), s);
    return db::execute_cql(req, ks_name, cf_name, sstable_name, int64_t(partition_size), key_str, timestamp)
    .then_wrapped([ks_name, cf_name, key_str, partition_size](auto&& f) {
        try {
            f.get();
            large_data_logger.warn("Writing large partition {}/{}:{} ({} bytes)", ks_name, cf_name, key_str, partition_size);
        } catch (...) {
            large_data_logger.warn("Failed to update {}: {}", db::system_keyspace::LARGE_PARTITIONS, std::current_exception());
        }
    });
}

future<> cql_table_large_data_handler::delete_large_partitions_entry(const schema& s, const sstring& sstable_name) const {
    static const sstring req = format("DELETE FROM system.{} WHERE keyspace_name = ? AND table_name = ? AND sstable_name = ?", db::system_keyspace::LARGE_PARTITIONS);
    return db::execute_cql(req, s.ks_name(), s.cf_name(), sstable_name).discard_result().handle_exception([](std::exception_ptr ep) {
            large_data_logger.warn("Failed to drop entries from {}: {}", db::system_keyspace::LARGE_PARTITIONS, ep);
        });
}

future<> cql_table_large_data_handler::record_large_rows(const sstables::sstable& sst, const sstables::key& partition_key,
        const clustering_key_prefix* clustering_key, uint64_t row_size) const {
    static const sstring req =
            format("INSERT INTO system.{} (keyspace_name, table_name, sstable_name, row_size, partition_key, "
                   "clustering_key, compaction_time) VALUES (?, ?, ?, ?, ?, ?, ?) USING TTL 2592000",
                    db::system_keyspace::LARGE_ROWS);
    auto f = [clustering_key, &partition_key, &sst, row_size] {
        const schema &s = *sst.get_schema();
        auto sstable_name = sst.get_filename();
        auto ks_name = s.ks_name();
        auto cf_name = s.cf_name();
        auto timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(db_clock::now().time_since_epoch()).count();
        std::string pk_str = key_to_str(partition_key.to_partition_key(s), s);
        if (clustering_key) {
            std::string ck_str = key_to_str(*clustering_key, s);
            large_data_logger.warn("Writing large row {}/{}: {} {} ({} bytes)", ks_name, cf_name, pk_str, ck_str, row_size);
            return db::execute_cql(req, ks_name, cf_name, sstable_name, int64_t(row_size), pk_str, ck_str, timestamp);
        } else {
            large_data_logger.warn("Writing large static row {}/{}: {} ({} bytes)", ks_name, cf_name, pk_str, row_size);
            return db::execute_cql(req, ks_name, cf_name, sstable_name, int64_t(row_size), pk_str, nullptr, timestamp);
        }
    };
    return f().discard_result().handle_exception([&sst] (std::exception_ptr ep) {
        const schema &s = *sst.get_schema();
        large_data_logger.warn("Failed to add a record to {}: ks = {}, table = {}, sst = {} exception = {}",
                db::system_keyspace::LARGE_ROWS, s.ks_name(), s.cf_name(), sst.get_filename(), ep);
    });
}

future<> cql_table_large_data_handler::delete_large_rows_entries(const schema& s, const sstring& sstable_name) const {
    static const sstring req =
            format("DELETE FROM system.{} WHERE keyspace_name = ? AND table_name = ? AND sstable_name = ?",
                    db::system_keyspace::LARGE_ROWS);
    return db::execute_cql(req, s.ks_name(), s.cf_name(), sstable_name)
            .discard_result()
            .handle_exception([&s, sstable_name] (std::exception_ptr ep) {
                large_data_logger.warn("Failed to drop entries from {}: ks = {}, table = {}, sst = {} exception = {}",
                        db::system_keyspace::LARGE_ROWS, s.ks_name(), s.cf_name(), sstable_name, ep);
            });
}
}
