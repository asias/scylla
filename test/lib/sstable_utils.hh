/*
 * Copyright (C) 2017 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include "sstables/sstables.hh"
#include "sstables/shared_sstable.hh"
#include "memtable-sstable.hh"
#include "dht/i_partitioner.hh"
#include <boost/range/irange.hpp>
#include <boost/range/adaptor/map.hpp>
#include "test/lib/test_services.hh"
#include "test/lib/sstable_test_env.hh"
#include "gc_clock.hh"

using namespace sstables;
using namespace std::chrono_literals;

struct local_shard_only_tag { };
using local_shard_only = bool_class<local_shard_only_tag>;

sstables::shared_sstable make_sstable_containing(std::function<sstables::shared_sstable()> sst_factory, std::vector<mutation> muts);

inline future<> write_memtable_to_sstable_for_test(memtable& mt, sstables::shared_sstable sst) {
    return write_memtable_to_sstable(mt, sst, test_sstables_manager.configure_writer());
}

//
// Make set of keys sorted by token for current or remote shard.
//
static std::vector<sstring> do_make_keys(unsigned n, const schema_ptr& s, size_t min_key_size = 1, local_shard_only lso = local_shard_only::yes) {
    std::vector<std::pair<sstring, dht::decorated_key>> p;
    p.reserve(n);

    auto key_id = 0U;
    auto generated = 0U;
    while (generated < n) {
        auto raw_key = sstring(std::max(min_key_size, sizeof(key_id)), int8_t(0));
        std::copy_n(reinterpret_cast<int8_t*>(&key_id), sizeof(key_id), raw_key.begin());
        auto dk = dht::decorate_key(*s, partition_key::from_single_value(*s, to_bytes(raw_key)));
        key_id++;
        if (lso) {
            if (engine_is_ready() && engine().cpu_id() != shard_of(*s, dk.token())) {
                continue;
            }
        }
        generated++;
        p.emplace_back(std::move(raw_key), std::move(dk));
    }
    boost::sort(p, [&] (auto& p1, auto& p2) {
        return p1.second.less_compare(*s, p2.second);
    });
    return boost::copy_range<std::vector<sstring>>(p | boost::adaptors::map_keys);
}

inline std::vector<sstring> make_local_keys(unsigned n, const schema_ptr& s, size_t min_key_size = 1) {
    return do_make_keys(n, s, min_key_size, local_shard_only::yes);
}

inline sstring make_local_key(const schema_ptr& s, size_t min_key_size = 1) {
    return do_make_keys(1, s, min_key_size, local_shard_only::yes).front();
}

inline std::vector<sstring> make_keys(unsigned n, const schema_ptr& s, size_t min_key_size = 1) {
    return do_make_keys(n, s, min_key_size, local_shard_only::no);
}

shared_sstable make_sstable(sstables::test_env& env, schema_ptr s, sstring dir, std::vector<mutation> mutations,
        sstable_writer_config cfg, sstables::sstable::version_types version, gc_clock::time_point query_time = gc_clock::now());
