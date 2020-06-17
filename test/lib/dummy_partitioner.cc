/*
 * Copyright (C) 2020 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#include <boost/range/algorithm/find.hpp>
#include "test/lib/dummy_partitioner.hh"

unsigned dummy_partitioner::shard_of(const dht::token& t) const {
    auto it = boost::find(_tokens, t);
    // Unknown tokens are assigned to shard 0
    return it == _tokens.end() ? 0 : std::distance(_tokens.begin(), it) % _partitioner.shard_count();
}

dht::token dummy_partitioner::token_for_next_shard(const dht::token& t, shard_id shard, unsigned spans) const {
    // Find the first token that belongs to `shard` and is larger than `t`
    auto it = std::find_if(_tokens.begin(), _tokens.end(), [this, &t, shard] (const dht::token& shard_token) {
        return shard_token > t && shard_of(shard_token) == shard;
    });

    if (it == _tokens.end()) {
        return dht::maximum_token();
    }

    --spans;

    while (spans) {
        if (std::distance(it, _tokens.end()) <= _partitioner.shard_count()) {
            return dht::maximum_token();
        }
        it += _partitioner.shard_count();
        --spans;
    }

    return *it;
}

