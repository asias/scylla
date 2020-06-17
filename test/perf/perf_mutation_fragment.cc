/*
 * Copyright (C) 2018 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#include "seastar/include/seastar/testing/perf_tests.hh"

#include "test/lib/simple_schema.hh"

#include "mutation_fragment.hh"

namespace tests {

class clustering_row {
    mutable simple_schema _schema;
    clustering_key _key;

    bytes _value_4;
    bytes _value_4k;
    bytes _value_1M;

    mutation_fragment _row_4;
    mutation_fragment _row_4k;
    mutation_fragment _row_1M;
public:
    clustering_row()
        : _key(_schema.make_ckey(0))
        , _value_4(4, 'a')
        , _value_4k(4 * 1024, 'b')
        , _value_1M(1024 * 1024, 'c')
        , _row_4(_schema.make_row_from_serialized_value(_key, _value_4))
        , _row_4k(_schema.make_row_from_serialized_value(_key, _value_4k))
        , _row_1M(_schema.make_row_from_serialized_value(_key, _value_1M))
    { }

    schema_ptr schema() const { return _schema.schema(); }

    mutation_fragment& clustering_row_4() {
        return _row_4;
    }

    mutation_fragment& clustering_row_4k() {
        return _row_4k;
    }

    mutation_fragment& clustering_row_1M() {
        return _row_1M;
    }

    mutation_fragment make_clustering_row_4() const {
        return _schema.make_row_from_serialized_value(_key, _value_4);
    }

    mutation_fragment make_clustering_row_4k() const {
        return _schema.make_row_from_serialized_value(_key, _value_4k);
    }

    mutation_fragment make_clustering_row_1M() const {
        return _schema.make_row_from_serialized_value(_key, _value_1M);
    }
};

PERF_TEST_F(clustering_row, make_4)
{
    auto mf = make_clustering_row_4();
    perf_tests::do_not_optimize(mf);
}

PERF_TEST_F(clustering_row, make_4k)
{
    auto mf = make_clustering_row_4k();
    perf_tests::do_not_optimize(mf);
}

PERF_TEST_F(clustering_row, make_1M)
{
    auto mf = make_clustering_row_1M();
    perf_tests::do_not_optimize(mf);
}

PERF_TEST_F(clustering_row, copy_4)
{
    auto mf = mutation_fragment(*schema(), clustering_row_4());
    perf_tests::do_not_optimize(mf);
}

PERF_TEST_F(clustering_row, copy_4k)
{
    auto mf = mutation_fragment(*schema(), clustering_row_4k());
    perf_tests::do_not_optimize(mf);
}

PERF_TEST_F(clustering_row, copy_1M)
{
    auto mf = mutation_fragment(*schema(), clustering_row_1M());
    perf_tests::do_not_optimize(mf);
}

PERF_TEST_F(clustering_row, hash_4)
{
    auto& cr = clustering_row_4().as_mutable_clustering_row();
    cr.cells().prepare_hash(*schema(), column_kind::regular_column);
    perf_tests::do_not_optimize(cr);
    cr.cells().clear_hash();
}

PERF_TEST_F(clustering_row, hash_4k)
{
    auto& cr = clustering_row_4k().as_mutable_clustering_row();
    cr.cells().prepare_hash(*schema(), column_kind::regular_column);
    perf_tests::do_not_optimize(cr);
    cr.cells().clear_hash();
}

PERF_TEST_F(clustering_row, hash_1M)
{
    auto& cr = clustering_row_1M().as_mutable_clustering_row();
    cr.cells().prepare_hash(*schema(), column_kind::regular_column);
    perf_tests::do_not_optimize(cr);
    cr.cells().clear_hash();
}

}
