/*
 * Copyright (C) 2015 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include "i_partitioner.hh"
#include "range.hh"

#include <vector>

namespace dht {

struct ring_position_range_and_shard {
    dht::partition_range ring_range;
    unsigned shard;
};

class ring_position_range_sharder {
    const i_partitioner& _partitioner;
    dht::partition_range _range;
    bool _done = false;
public:
    ring_position_range_sharder(const i_partitioner& partitioner, nonwrapping_range<ring_position> rrp)
            : _partitioner(partitioner), _range(std::move(rrp)) {}
    std::optional<ring_position_range_and_shard> next(const schema& s);
};

struct ring_position_range_and_shard_and_element : ring_position_range_and_shard {
    ring_position_range_and_shard_and_element(ring_position_range_and_shard&& rpras, unsigned element)
            : ring_position_range_and_shard(std::move(rpras)), element(element) {
    }
    unsigned element;
};

struct ring_position_exponential_sharder_result {
    std::vector<ring_position_range_and_shard> per_shard_ranges;
    bool inorder = true;
};

// given a ring_position range, generates exponentially increasing
// sets per-shard sub-ranges
class ring_position_exponential_sharder {
    const i_partitioner& _partitioner;
    partition_range _range;
    unsigned _spans_per_iteration = 1;
    unsigned _first_shard = 0;
    unsigned _next_shard = 0;
    std::vector<std::optional<token>> _last_ends; // index = shard
public:
    explicit ring_position_exponential_sharder(partition_range pr);
    explicit ring_position_exponential_sharder(const i_partitioner& partitioner, partition_range pr);
    std::optional<ring_position_exponential_sharder_result> next(const schema& s);
};

class ring_position_range_vector_sharder {
    using vec_type = dht::partition_range_vector;
    vec_type _ranges;
    const i_partitioner& _partitioner;
    vec_type::iterator _current_range;
    std::optional<ring_position_range_sharder> _current_sharder;
private:
    void next_range() {
        if (_current_range != _ranges.end()) {
            _current_sharder.emplace(_partitioner, std::move(*_current_range++));
        }
    }
public:
    ring_position_range_vector_sharder(const dht::i_partitioner& p, dht::partition_range_vector ranges);
    // results are returned sorted by index within the vector first, then within each vector item
    std::optional<ring_position_range_and_shard_and_element> next(const schema& s);
};

class selective_token_range_sharder {
    const i_partitioner& _partitioner;
    dht::token_range _range;
    shard_id _shard;
    bool _done = false;
    shard_id _next_shard;
    dht::token _start_token;
    std::optional<range_bound<dht::token>> _start_boundary;
public:
    selective_token_range_sharder(const i_partitioner& partitioner, dht::token_range range, shard_id shard)
            : _partitioner(partitioner)
            , _range(std::move(range))
            , _shard(shard)
            , _next_shard(_shard + 1 == _partitioner.shard_count() ? 0 : _shard + 1)
            , _start_token(_range.start() ? _range.start()->value() : minimum_token())
            , _start_boundary(_partitioner.shard_of(_start_token) == shard ?
                _range.start() : range_bound<dht::token>(_partitioner.token_for_next_shard(_start_token, shard))) {
    }
    std::optional<dht::token_range> next();
};

} // dht
