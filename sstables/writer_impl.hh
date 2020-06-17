/*
 * Copyright (C) 2018 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include "sstables.hh"
#include "schema_fwd.hh"
#include "mutation_fragment.hh"

namespace sstables {

struct sstable_writer::writer_impl {
    sstable& _sst;
    const schema& _schema;
    const io_priority_class& _pc;
    const sstable_writer_config _cfg;

    writer_impl(sstable& sst, const schema& schema, const io_priority_class& pc, const sstable_writer_config& cfg)
        : _sst(sst)
        , _schema(schema)
        , _pc(pc)
        , _cfg(cfg)
    {}

    virtual void consume_new_partition(const dht::decorated_key& dk) = 0;
    virtual void consume(tombstone t) = 0;
    virtual stop_iteration consume(static_row&& sr) = 0;
    virtual stop_iteration consume(clustering_row&& cr) = 0;
    virtual stop_iteration consume(range_tombstone&& rt) = 0;
    virtual stop_iteration consume_end_of_partition() = 0;
    virtual void consume_end_of_stream() = 0;
    virtual ~writer_impl() {}
};

}
