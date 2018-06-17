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
#include "memtable-sstable.hh"
#include "dht/i_partitioner.hh"
#include <boost/range/irange.hpp>
#include <boost/range/adaptor/map.hpp>

sstables::shared_sstable make_sstable_containing(std::function<sstables::shared_sstable()> sst_factory, std::vector<mutation> muts);

inline future<> write_memtable_to_sstable_for_test(memtable& mt, sstables::shared_sstable sst) {
    static db::nop_large_partition_handler nop_lp_handler;
    return write_memtable_to_sstable(mt, sst, &nop_lp_handler);
}

//
// Make set of keys sorted by token for current shard.
//
static std::vector<sstring> make_local_keys(unsigned n, const schema_ptr& s, size_t min_key_size = 1) {
    std::vector<std::pair<sstring, dht::decorated_key>> p;
    p.reserve(n);

    auto key_id = 0U;
    auto generated = 0U;
    while (generated < n) {
        auto raw_key = sstring(std::max(min_key_size, sizeof(key_id)), int8_t(0));
        std::copy_n(reinterpret_cast<int8_t*>(&key_id), sizeof(key_id), raw_key.begin());
        auto dk = dht::global_partitioner().decorate_key(*s, partition_key::from_single_value(*s, to_bytes(raw_key)));
        key_id++;

        if (engine_is_ready() && engine().cpu_id() != dht::global_partitioner().shard_of(dk.token())) {
            continue;
        }
        generated++;
        p.emplace_back(std::move(raw_key), std::move(dk));
    }
    boost::sort(p, [&] (auto& p1, auto& p2) {
        return p1.second.less_compare(*s, p2.second);
    });
    return boost::copy_range<std::vector<sstring>>(p | boost::adaptors::map_keys);
}

//
// Return one key for current shard. Note that it always returns the same key for a given shard.
//
inline sstring make_local_key(const schema_ptr& s, size_t min_key_size = 1) {
    return make_local_keys(1, s, min_key_size).front();
}


