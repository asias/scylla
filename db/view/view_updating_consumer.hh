/*
 * Copyright (C) 2018 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include "dht/i_partitioner.hh"
#include "schema_fwd.hh"
#include "mutation_fragment.hh"
#include "sstables/shared_sstable.hh"
#include "database.hh"

namespace db::view {

/*
 * A consumer that pushes materialized view updates for each consumed mutation.
 * It is expected to be run in seastar::async threaded context through consume_in_thread()
 */
class view_updating_consumer {
public:
    // We prefer flushing on partition boundaries, so at the end of a partition,
    // we flush on reaching the soft limit. Otherwise we continue accumulating
    // data. We flush mid-partition if we reach the hard limit.
    static const size_t buffer_size_soft_limit;
    static const size_t buffer_size_hard_limit;

private:
    schema_ptr _schema;
    lw_shared_ptr<table> _table;
    sstables::shared_sstable _excluded_sstable;
    const seastar::abort_source& _as;
    circular_buffer<mutation> _buffer;
    mutation* _m{nullptr};
    size_t _buffer_size{0};

private:
    void do_flush_buffer();
    void maybe_flush_buffer_mid_partition();

public:
    view_updating_consumer(schema_ptr schema, database& db, sstables::shared_sstable excluded_sstable, const seastar::abort_source& as)
            : _schema(std::move(schema))
            , _table(db.find_column_family(_schema->id()).shared_from_this())
            , _excluded_sstable(excluded_sstable)
            , _as(as)
            , _m()
    { }

    view_updating_consumer(view_updating_consumer&&) = default;

    view_updating_consumer& operator=(view_updating_consumer&&) = delete;

    void consume_new_partition(const dht::decorated_key& dk) {
        _buffer.emplace_back(_schema, dk, mutation_partition(_schema));
        _m = &_buffer.back();
    }

    void consume(tombstone t) {
        _m->partition().apply(std::move(t));
    }

    stop_iteration consume(static_row&& sr) {
        if (_as.abort_requested()) {
            return stop_iteration::yes;
        }
        _buffer_size += sr.memory_usage(*_schema);
        _m->partition().apply(*_schema, std::move(sr));
        maybe_flush_buffer_mid_partition();
        return stop_iteration::no;
    }

    stop_iteration consume(clustering_row&& cr) {
        if (_as.abort_requested()) {
            return stop_iteration::yes;
        }
        _buffer_size += cr.memory_usage(*_schema);
        _m->partition().apply(*_schema, std::move(cr));
        maybe_flush_buffer_mid_partition();
        return stop_iteration::no;
    }

    stop_iteration consume(range_tombstone&& rt) {
        if (_as.abort_requested()) {
            return stop_iteration::yes;
        }
        _buffer_size += rt.memory_usage(*_schema);
        _m->partition().apply(*_schema, std::move(rt));
        maybe_flush_buffer_mid_partition();
        return stop_iteration::no;
    }

    // Expected to be run in seastar::async threaded context (consume_in_thread())
    stop_iteration consume_end_of_partition() {
        if (_as.abort_requested()) {
            return stop_iteration::yes;
        }
        if (_buffer_size >= buffer_size_soft_limit) {
            do_flush_buffer();
        }
        return stop_iteration::no;
    }

    stop_iteration consume_end_of_stream() {
        if (!_buffer.empty()) {
            do_flush_buffer();
        }
        return stop_iteration(_as.abort_requested());
    }
};

}

