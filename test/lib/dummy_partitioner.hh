/*
 * Copyright (C) 2020 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include <boost/range/adaptor/map.hpp>
#include "dht/token.hh"
#include "dht/i_partitioner.hh"

// Shards tokens such that tokens are owned by shards in a round-robin manner.
class dummy_partitioner : public dht::i_partitioner {
    const dht::i_partitioner& _partitioner;
    std::vector<dht::token> _tokens;

public:
    // We need a container input that enforces token order by design.
    // In addition client code will often map tokens to something, e.g. mutation
    // they originate from or shards, etc. So, for convenience we allow any
    // ordered associative container (std::map) that has dht::token as keys.
    // Values will be ignored.
    template <typename T>
    dummy_partitioner(const dht::i_partitioner& partitioner, const std::map<dht::token, T>& something_by_token)
        : i_partitioner(smp::count)
        , _partitioner(partitioner)
        , _tokens(boost::copy_range<std::vector<dht::token>>(something_by_token | boost::adaptors::map_keys)) {
    }

    virtual dht::token get_token(const schema& s, partition_key_view key) const override { return _partitioner.get_token(s, key); }
    virtual dht::token get_token(const sstables::key_view& key) const override { return _partitioner.get_token(key); }
    virtual bool preserves_order() const override { return _partitioner.preserves_order(); }
    virtual const sstring name() const override { return _partitioner.name(); }
    virtual unsigned shard_of(const dht::token& t) const override;
    virtual dht::token token_for_next_shard(const dht::token& t, shard_id shard, unsigned spans = 1) const override;
};

