/*
 * Copyright 2016 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include <experimental/optional>

#include "frozen_mutation.hh"
#include "schema.hh"
#include "stdx.hh"

class commitlog_entry {
    stdx::optional<column_mapping> _mapping;
    frozen_mutation _mutation;
public:
    commitlog_entry(stdx::optional<column_mapping> mapping, frozen_mutation&& mutation)
        : _mapping(std::move(mapping)), _mutation(std::move(mutation)) { }
    const stdx::optional<column_mapping>& mapping() const { return _mapping; }
    const frozen_mutation& mutation() const & { return _mutation; }
    frozen_mutation&& mutation() && { return std::move(_mutation); }
};

class commitlog_entry_writer {
    schema_ptr _schema;
    const frozen_mutation& _mutation;
    bool _with_schema = true;
    size_t _size = std::numeric_limits<size_t>::max();
private:
    template<typename Output>
    void serialize(Output&) const;
    void compute_size();
public:
    commitlog_entry_writer(schema_ptr s, const frozen_mutation& fm)
        : _schema(std::move(s)), _mutation(fm)
    {}

    void set_with_schema(bool value) {
        _with_schema = value;
        compute_size();
    }
    bool with_schema() {
        return _with_schema;
    }
    schema_ptr schema() const {
        return _schema;
    }

    size_t size() const {
        assert(_size != std::numeric_limits<size_t>::max());
        return _size;
    }

    size_t mutation_size() const {
        return _mutation.representation().size();
    }

    void write(typename seastar::memory_output_stream<std::vector<temporary_buffer<char>>::iterator>& out) const;
};

class commitlog_entry_reader {
    commitlog_entry _ce;
public:
    commitlog_entry_reader(const fragmented_temporary_buffer& buffer);

    const stdx::optional<column_mapping>& get_column_mapping() const { return _ce.mapping(); }
    const frozen_mutation& mutation() const & { return _ce.mutation(); }
    frozen_mutation&& mutation() && { return std::move(_ce).mutation(); }
};
