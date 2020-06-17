/*
 * Copyright (C) 2020 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include "dht/token.hh"

namespace dht {

inline sstring cpu_sharding_algorithm_name() {
    return "biased-token-round-robin";
}

std::vector<uint64_t> init_zero_based_shard_start(unsigned shards, unsigned sharding_ignore_msb_bits);

unsigned shard_of(unsigned shard_count, unsigned sharding_ignore_msb_bits, const token& t);

token token_for_next_shard(const std::vector<uint64_t>& shard_start, unsigned shard_count, unsigned sharding_ignore_msb_bits, const token& t, shard_id shard, unsigned spans);

} //namespace dht
