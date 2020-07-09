/*
 * Copyright (C) 2018 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#include <boost/test/unit_test.hpp>

#include "database.hh"
#include "db/view/view_builder.hh"
#include "db/system_keyspace.hh"

#include <seastar/testing/test_case.hh>
#include <seastar/testing/thread_test_case.hh>
#include "test/lib/cql_test_env.hh"
#include "test/lib/cql_assertions.hh"
#include "schema_builder.hh"
#include "service/priority_manager.hh"
#include "test/lib/test_services.hh"
#include "test/lib/data_model.hh"

using namespace std::literals::chrono_literals;

schema_ptr test_table_schema() {
    static thread_local auto s = [] {
        schema_builder builder(make_lw_shared(schema(
                generate_legacy_id("try1", "data"), "try1", "data",
        // partition key
        {{"p", utf8_type}},
        // clustering key
        {{"c", utf8_type}},
        // regular columns
        {{"v", utf8_type}},
        // static columns
        {},
        // regular column name type
        utf8_type,
        // comment
        ""
       )));
       return builder.build(schema_builder::compact_storage::no);
    }();
    return s;
}

SEASTAR_TEST_CASE(test_builder_with_large_partition) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        e.execute_cql("create table cf (p int, c int, v int, primary key (p, c))").get();

        for (auto i = 0; i < 1024; ++i) {
            e.execute_cql(format("insert into cf (p, c, v) values (0, {:d}, 0)", i)).get();
        }

        auto f = e.local_view_builder().wait_until_built("ks", "vcf");
        e.execute_cql("create materialized view vcf as select * from cf "
                      "where p is not null and c is not null and v is not null "
                      "primary key (v, c, p)").get();

        f.get();
        auto built = db::system_keyspace::load_built_views().get0();
        BOOST_REQUIRE_EQUAL(built.size(), 1);
        BOOST_REQUIRE_EQUAL(built[0].second, sstring("vcf"));

        auto msg = e.execute_cql("select count(*) from vcf where v = 0").get0();
        assert_that(msg).is_rows().with_size(1);
        assert_that(msg).is_rows().with_rows({{{long_type->decompose(1024L)}}});
    });
}

// This test reproduces issue #4213. We have a large base partition with
// many rows, and the view has the *same* partition key as the base, so all
// the generated view rows will go to the same view partition. The view
// builder used to batch up to 128 (view_builder::batch_size) of these view
// rows into one view mutation, and if the individual rows are big (i.e.,
// contain very long strings or blobs), this 128-row mutation can be too
// large to be applied because of our limitation on commit-log segment size
// (commitlog_segment_size_in_mb, by default 32 MB). When applying the
// mutation fails, view building retries indefinitely and this test hung.
SEASTAR_TEST_CASE(test_builder_with_large_partition_2) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        const int nrows = 64; // meant to be lower than view_builder::batch_size
        const int target_size = 33*1024*1024; // meant to be higher than commitlog_segment_size_in_mb
        e.execute_cql("create table cf (p int, c int, s ascii, primary key (p, c))").get();
        const sstring longstring = sstring(target_size / nrows, 'x');
        for (auto i = 0; i < nrows; ++i) {
            e.execute_cql(format("insert into cf (p, c, s) values (0, {:d}, '{}')", i, longstring)).get();
        }

        auto f = e.local_view_builder().wait_until_built("ks", "vcf");
        e.execute_cql("create materialized view vcf as select * from cf "
                      "where p is not null and c is not null "
                      "primary key (p, c)").get();

        f.get();
        auto built = db::system_keyspace::load_built_views().get0();
        BOOST_REQUIRE_EQUAL(built.size(), 1);
        BOOST_REQUIRE_EQUAL(built[0].second, sstring("vcf"));

        auto msg = e.execute_cql("select count(*) from vcf").get0();
        assert_that(msg).is_rows().with_size(1);
        assert_that(msg).is_rows().with_rows({{{long_type->decompose(long(nrows))}}});
    });
}


SEASTAR_TEST_CASE(test_builder_with_multiple_partitions) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        e.execute_cql("create table cf (p int, c int, v int, primary key (p, c))").get();

        for (auto i = 0; i < 1024; ++i) {
            e.execute_cql(format("insert into cf (p, c, v) values ({:d}, {:d}, 0)", i % 5, i)).get();
        }

        auto f = e.local_view_builder().wait_until_built("ks", "vcf");
        e.execute_cql("create materialized view vcf as select * from cf "
                      "where p is not null and c is not null and v is not null "
                      "primary key (v, c, p)").get();

        f.get();
        auto built = db::system_keyspace::load_built_views().get0();
        BOOST_REQUIRE_EQUAL(built.size(), 1);
        BOOST_REQUIRE_EQUAL(built[0].second, sstring("vcf"));

        auto msg = e.execute_cql("select count(*) from vcf where v = 0").get0();
        assert_that(msg).is_rows().with_size(1);
        assert_that(msg).is_rows().with_rows({{{long_type->decompose(1024L)}}});
    });
}

SEASTAR_TEST_CASE(test_builder_with_multiple_partitions_of_batch_size_rows) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        e.execute_cql("create table cf (p int, c int, v int, primary key (p, c))").get();

        for (auto i = 0; i < 1024; ++i) {
            e.execute_cql(format("insert into cf (p, c, v) values ({:d}, {:d}, 0)", i % db::view::view_builder::batch_size, i)).get();
        }

        auto f = e.local_view_builder().wait_until_built("ks", "vcf");
        e.execute_cql("create materialized view vcf as select * from cf "
                      "where p is not null and c is not null and v is not null "
                      "primary key (v, c, p)").get();

        f.get();
        auto built = db::system_keyspace::load_built_views().get0();
        BOOST_REQUIRE_EQUAL(built.size(), 1);
        BOOST_REQUIRE_EQUAL(built[0].second, sstring("vcf"));

        auto msg = e.execute_cql("select count(*) from vcf where v = 0").get0();
        assert_that(msg).is_rows().with_size(1);
        assert_that(msg).is_rows().with_rows({{{long_type->decompose(1024L)}}});
    });
}

SEASTAR_TEST_CASE(test_builder_view_added_during_ongoing_build) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        e.execute_cql("create table cf (p int, c int, v int, primary key (p, c))").get();

        for (auto i = 0; i < 5000; ++i) {
            e.execute_cql(format("insert into cf (p, c, v) values (0, {:d}, 0)", i)).get();
        }

        auto f1 = e.local_view_builder().wait_until_built("ks", "vcf1");
        auto f2 = e.local_view_builder().wait_until_built("ks", "vcf2");

        e.execute_cql("create materialized view vcf1 as select * from cf "
                      "where p is not null and c is not null and v is not null "
                      "primary key (v, c, p)").get();

        sleep(1s).get();

        e.execute_cql("create materialized view vcf2 as select * from cf "
                      "where p is not null and c is not null and v is not null "
                      "primary key (v, p, c)").get();

        f2.get();
        f1.get();
        auto built = db::system_keyspace::load_built_views().get0();
        BOOST_REQUIRE_EQUAL(built.size(), 2);

        auto msg = e.execute_cql("select count(*) from vcf1 where v = 0").get0();
        assert_that(msg).is_rows().with_size(1);
        assert_that(msg).is_rows().with_rows({{{long_type->decompose(5000L)}}});

        msg = e.execute_cql("select count(*) from vcf2 where v = 0").get0();
        assert_that(msg).is_rows().with_size(1);
        assert_that(msg).is_rows().with_rows({{{long_type->decompose(5000L)}}});
    });
}

std::mt19937 random_generator() {
    std::random_device rd;
    // In case of errors, replace the seed with a fixed value to get a deterministic run.
    auto seed = rd();
    std::cout << "Random seed: " << seed << "\n";
    return std::mt19937(seed);
}

bytes random_bytes(size_t size, std::mt19937& gen) {
    bytes result(bytes::initialized_later(), size);
    static thread_local std::uniform_int_distribution<int> dist(0, std::numeric_limits<uint8_t>::max());
    for (size_t i = 0; i < size; ++i) {
        result[i] = dist(gen);
    }
    return result;
}

SEASTAR_TEST_CASE(test_builder_across_tokens_with_large_partitions) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        auto gen = random_generator();

        e.execute_cql("create table cf (p blob, c int, v int, primary key (p, c))").get();
        auto s = e.local_db().find_schema("ks", "cf");

        auto make_key = [&] (auto) { return to_hex(random_bytes(128, gen));  };
        for (auto&& k : boost::irange(0, 4) | boost::adaptors::transformed(make_key)) {
            for (auto i = 0; i < 1000; ++i) {
                e.execute_cql(format("insert into cf (p, c, v) values (0x{}, {:d}, 0)", k, i)).get();
            }
        }

        auto f1 = e.local_view_builder().wait_until_built("ks", "vcf1");
        auto f2 = e.local_view_builder().wait_until_built("ks", "vcf2");

        e.execute_cql("create materialized view vcf1 as select * from cf "
                      "where p is not null and c is not null and v is not null "
                      "primary key (v, c, p)").get();

        sleep(1s).get();

        e.execute_cql("create materialized view vcf2 as select * from cf "
                      "where p is not null and c is not null and v is not null "
                      "primary key (v, p, c)").get();

        f2.get();
        f1.get();
        auto built = db::system_keyspace::load_built_views().get0();
        BOOST_REQUIRE_EQUAL(built.size(), 2);

        auto msg = e.execute_cql("select count(*) from vcf1 where v = 0").get0();
        assert_that(msg).is_rows().with_size(1);
        assert_that(msg).is_rows().with_rows({{{long_type->decompose(4000L)}}});

        msg = e.execute_cql("select count(*) from vcf2 where v = 0").get0();
        assert_that(msg).is_rows().with_size(1);
        assert_that(msg).is_rows().with_rows({{{long_type->decompose(4000L)}}});
    });
}

SEASTAR_TEST_CASE(test_builder_across_tokens_with_small_partitions) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        auto gen = random_generator();

        e.execute_cql("create table cf (p blob, c int, v int, primary key (p, c))").get();
        auto s = e.local_db().find_schema("ks", "cf");

        auto make_key = [&] (auto) { return to_hex(random_bytes(128, gen));  };
        for (auto&& k : boost::irange(0, 1000) | boost::adaptors::transformed(make_key)) {
            for (auto i = 0; i < 4; ++i) {
                e.execute_cql(format("insert into cf (p, c, v) values (0x{}, {:d}, 0)", k, i)).get();
            }
        }

        auto f1 = e.local_view_builder().wait_until_built("ks", "vcf1");
        auto f2 = e.local_view_builder().wait_until_built("ks", "vcf2");

        e.execute_cql("create materialized view vcf1 as select * from cf "
                      "where p is not null and c is not null and v is not null "
                      "primary key (v, c, p)").get();

        sleep(1s).get();

        e.execute_cql("create materialized view vcf2 as select * from cf "
                      "where p is not null and c is not null and v is not null "
                      "primary key (v, p, c)").get();

        f2.get();
        f1.get();

        auto built = db::system_keyspace::load_built_views().get0();
        BOOST_REQUIRE_EQUAL(built.size(), 2);

        auto msg = e.execute_cql("select count(*) from vcf1 where v = 0").get0();
        assert_that(msg).is_rows().with_size(1);
        assert_that(msg).is_rows().with_rows({{{long_type->decompose(4000L)}}});

        msg = e.execute_cql("select count(*) from vcf2 where v = 0").get0();
        assert_that(msg).is_rows().with_size(1);
        assert_that(msg).is_rows().with_rows({{{long_type->decompose(4000L)}}});
    });
}

SEASTAR_TEST_CASE(test_builder_with_tombstones) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        e.execute_cql("create table cf (p int, c1 int, c2 int, v int, primary key (p, c1, c2))").get();

        for (auto i = 0; i < 100; ++i) {
            e.execute_cql(format("insert into cf (p, c1, c2, v) values (0, {:d}, {:d}, 1)", i % 2, i)).get();
        }

        e.execute_cql("delete from cf where p = 0 and c1 = 0").get();
        e.execute_cql("delete from cf where p = 0 and c1 = 1 and c2 >= 50 and c2 < 101").get();

        auto f = e.local_view_builder().wait_until_built("ks", "vcf");
        e.execute_cql("create materialized view vcf as select * from cf "
                      "where p is not null and c1 is not null and c2 is not null and v is not null "
                      "primary key ((v, p), c1, c2)").get();

        f.get();
        auto built = db::system_keyspace::load_built_views().get0();
        BOOST_REQUIRE_EQUAL(built.size(), 1);

        auto msg = e.execute_cql("select * from vcf").get0();
        assert_that(msg).is_rows().with_size(25);
    });
}

SEASTAR_TEST_CASE(test_builder_with_concurrent_writes) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        auto gen = random_generator();

        e.execute_cql("create table cf (p blob, c int, v int, primary key (p, c))").get();

        const size_t rows = 6864;
        const size_t rows_per_partition = 4;
        const size_t partitions = rows / rows_per_partition;

        std::unordered_set<sstring> keys;
        while (keys.size() != partitions) {
            keys.insert(to_hex(random_bytes(128, gen)));
        }

        auto half = keys.begin();
        std::advance(half, keys.size() / 2);
        auto k = keys.begin();
        for (; k != half; ++k) {
            for (size_t i = 0; i < rows_per_partition; ++i) {
                e.execute_cql(format("insert into cf (p, c, v) values (0x{}, {:d}, 0)", *k, i)).get();
            }
        }

        auto f = e.local_view_builder().wait_until_built("ks", "vcf");
        e.execute_cql("create materialized view vcf as select * from cf "
                      "where p is not null and c is not null and v is not null "
                      "primary key (v, c, p)").get();

        for (; k != keys.end(); ++k) {
            for (size_t i = 0; i < rows_per_partition; ++i) {
                e.execute_cql(format("insert into cf (p, c, v) values (0x{}, {:d}, 0)", *k, i)).get();
            }
        }

        f.get();
        eventually([&] {
            auto msg = e.execute_cql("select * from vcf").get0();
            assert_that(msg).is_rows().with_size(rows);
        });
    });
}

SEASTAR_TEST_CASE(test_builder_with_concurrent_drop) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        auto gen = random_generator();

        e.execute_cql("create table cf (p blob, c int, v int, primary key (p, c))").get();

        auto make_key = [&] (auto) { return to_hex(random_bytes(128, gen));  };
        for (auto&& k : boost::irange(0, 1000) | boost::adaptors::transformed(make_key)) {
            for (auto i = 0; i < 5; ++i) {
                e.execute_cql(format("insert into cf (p, c, v) values (0x{}, {:d}, 0)", k, i)).get();
            }
        }

        e.execute_cql("create materialized view vcf as select * from cf "
                      "where p is not null and c is not null and v is not null "
                      "primary key (v, c, p)").get();

        e.execute_cql("drop materialized view vcf").get();

        eventually([&] {
            auto msg = e.execute_cql("select * from system.scylla_views_builds_in_progress").get0();
            assert_that(msg).is_rows().is_empty();
            msg = e.execute_cql("select * from system.built_views").get0();
            assert_that(msg).is_rows().is_empty();
            msg = e.execute_cql("select * from system.views_builds_in_progress").get0();
            assert_that(msg).is_rows().is_empty();
            msg = e.execute_cql("select * from system_distributed.view_build_status").get0();
            assert_that(msg).is_rows().is_empty();
        });
    });
}

SEASTAR_TEST_CASE(test_view_update_generator) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        e.execute_cql("create table t (p text, c text, v text, primary key (p, c))").get();
        for (auto i = 0; i < 1024; ++i) {
            e.execute_cql(fmt::format("insert into t (p, c, v) values ('a', 'c{}', 'x')", i)).get();
        }
        for (auto i = 0; i < 512; ++i) {
            e.execute_cql(fmt::format("insert into t (p, c, v) values ('b', 'c{}', '{}')", i, i + 1)).get();
        }
        auto& view_update_generator = e.local_view_update_generator();
        auto s = test_table_schema();

        auto key = partition_key::from_exploded(*s, {to_bytes("a")});
        mutation m(s, key);
        auto col = s->get_column_definition("v");
        for (int i = 1024; i < 1280; ++i) {
            auto& row = m.partition().clustered_row(*s, clustering_key::from_exploded(*s, {to_bytes(fmt::format("c{}", i))}));
            row.cells().apply(*col, atomic_cell::make_live(*col->type, 2345, col->type->decompose(sstring(fmt::format("v{}", i)))));
        }
        lw_shared_ptr<table> t = e.local_db().find_column_family("ks", "t").shared_from_this();

        auto sst = t->make_streaming_staging_sstable();
        sstables::sstable_writer_config sst_cfg = test_sstables_manager.configure_writer();
        auto& pc = service::get_local_streaming_write_priority();
        sst->write_components(flat_mutation_reader_from_mutations({m}), 1ul, s, sst_cfg, {}, pc).get();
        sst->open_data().get();
        t->add_sstable_and_update_cache(sst).get();
        view_update_generator.register_staging_sstable(sst, t).get();

        eventually([&] {
            auto msg = e.execute_cql("SELECT * FROM t WHERE p = 'a'").get0();
            assert_that(msg).is_rows().with_size(1280);
            msg = e.execute_cql("SELECT * FROM t WHERE p = 'b'").get0();
            assert_that(msg).is_rows().with_size(512);

            for (int i = 0; i < 1024; ++i) {
                auto msg = e.execute_cql(fmt::format("SELECT * FROM t WHERE p = 'a' and c = 'c{}'", i)).get0();
                assert_that(msg).is_rows().with_size(1).with_row({
                     {utf8_type->decompose(sstring("a"))},
                     {utf8_type->decompose(sstring(fmt::format("c{}", i)))},
                     {utf8_type->decompose(sstring("x"))}
                 });

            }
            for (int i = 1024; i < 1280; ++i) {
                auto msg = e.execute_cql(fmt::format("SELECT * FROM t WHERE p = 'a' and c = 'c{}'", i)).get0();
                assert_that(msg).is_rows().with_size(1).with_row({
                     {utf8_type->decompose(sstring("a"))},
                     {utf8_type->decompose(sstring(fmt::format("c{}", i)))},
                     {utf8_type->decompose(sstring(fmt::format("v{}", i)))}
                 });

            }
        });
    });
}

SEASTAR_THREAD_TEST_CASE(test_view_update_generator_buffering) {
    using partition_size_map = std::map<dht::decorated_key, size_t, dht::ring_position_less_comparator>;

    class consumer_verifier {
        schema_ptr _schema;
        const partition_size_map& _partition_rows;
        std::vector<mutation>& _collected_muts;
        bool& _failed;
        std::unique_ptr<row_locker> _rl;
        std::unique_ptr<row_locker::stats> _rl_stats;
        clustering_key::less_compare _less_cmp;
        const size_t _max_rows_soft;
        const size_t _max_rows_hard;
        size_t _buffer_rows = 0;

    private:
        static size_t rows_in_limit(size_t l) {
            const size_t _100kb = 100 * 1024;
            // round up
            return l / _100kb + std::min(size_t(1), l % _100kb);
        }

        static size_t rows_in_mut(const mutation& m) {
            return std::distance(m.partition().clustered_rows().begin(), m.partition().clustered_rows().end());
        }

        void check(mutation mut) {
            const size_t current_rows = rows_in_mut(mut);
            const auto total_rows = _partition_rows.at(mut.decorated_key());
            _buffer_rows += current_rows;

            BOOST_TEST_MESSAGE(fmt::format("consumer_verifier::check(): key={}, rows={}/{}, _buffer={}",
                    partition_key::with_schema_wrapper(*_schema, mut.key()),
                    current_rows,
                    total_rows,
                    _buffer_rows));

            BOOST_REQUIRE(current_rows);
            BOOST_REQUIRE(current_rows <= _max_rows_hard);
            BOOST_REQUIRE(_buffer_rows <= _max_rows_hard);

            // The current partition doesn't have all of its rows yet, verify
            // that the new mutation contains the next rows for the same
            // partition
            if (!_collected_muts.empty() && rows_in_mut(_collected_muts.back()) < _partition_rows.at(_collected_muts.back().decorated_key())) {
                BOOST_REQUIRE(_collected_muts.back().decorated_key().equal(*mut.schema(), mut.decorated_key()));
                const auto& previous_ckey = (--_collected_muts.back().partition().clustered_rows().end())->key();
                const auto& next_ckey = mut.partition().clustered_rows().begin()->key();
                BOOST_REQUIRE(_less_cmp(previous_ckey, next_ckey));
                mutation_application_stats stats;
                _collected_muts.back().partition().apply(*_schema, mut.partition(), *mut.schema(), stats);
            // The new mutation is a new partition.
            } else {
                if (!_collected_muts.empty()) {
                    BOOST_REQUIRE(!_collected_muts.back().decorated_key().equal(*mut.schema(), mut.decorated_key()));
                }
                _collected_muts.push_back(std::move(mut));
            }

            if (_buffer_rows >= _max_rows_hard) { // buffer flushed on hard limit
                _buffer_rows = 0;
                BOOST_TEST_MESSAGE(fmt::format("consumer_verifier::check(): buffer ends on hard limit"));
            } else if (_buffer_rows >= _max_rows_soft) { // buffer flushed on soft limit
                _buffer_rows = 0;
                BOOST_TEST_MESSAGE(fmt::format("consumer_verifier::check(): buffer ends on soft limit"));
            }
        }

    public:
        consumer_verifier(schema_ptr schema, const partition_size_map& partition_rows, std::vector<mutation>& collected_muts, bool& failed)
            : _schema(std::move(schema))
            , _partition_rows(partition_rows)
            , _collected_muts(collected_muts)
            , _failed(failed)
            , _rl(std::make_unique<row_locker>(_schema))
            , _rl_stats(std::make_unique<row_locker::stats>())
            , _less_cmp(*_schema)
            , _max_rows_soft(rows_in_limit(db::view::view_updating_consumer::buffer_size_soft_limit))
            , _max_rows_hard(rows_in_limit(db::view::view_updating_consumer::buffer_size_hard_limit))
        { }

        future<row_locker::lock_holder> operator()(mutation mut) {
            try {
                check(std::move(mut));
            } catch (...) {
                BOOST_TEST_MESSAGE(fmt::format("consumer_verifier::operator(): caught unexpected exception {}", std::current_exception()));
                _failed |= true;
            }
            return _rl->lock_pk(_collected_muts.back().decorated_key(), true, db::no_timeout, *_rl_stats);
        }
    };

    auto schema = schema_builder("ks", "cf")
            .with_column("pk", int32_type, column_kind::partition_key)
            .with_column("ck", int32_type, column_kind::clustering_key)
            .with_column("v", bytes_type)
            .build();

    const auto blob_100kb = bytes(100 * 1024, bytes::value_type(0xab));
    const abort_source as;

    const auto partition_size_sets = std::vector<std::vector<int>>{{12}, {8, 4}, {8, 16}, {22}, {8, 8, 8, 8}, {8, 8, 8, 16, 8}, {8, 20, 16, 16}, {50}, {21}, {21, 2}};
    const auto max_partition_set_size = boost::max_element(partition_size_sets, [] (const std::vector<int>& a, const std::vector<int>& b) { return a.size() < b.size(); })->size();
    auto pkeys = boost::copy_range<std::vector<dht::decorated_key>>(boost::irange(size_t{0}, max_partition_set_size) | boost::adaptors::transformed([schema] (int i) {
        return dht::decorate_key(*schema, partition_key::from_single_value(*schema, int32_type->decompose(data_value(i))));
    }));
    boost::sort(pkeys, dht::ring_position_less_comparator(*schema));

    for (auto partition_sizes_100kb : partition_size_sets) {
        BOOST_TEST_MESSAGE(fmt::format("partition_sizes_100kb={}", partition_sizes_100kb));
        partition_size_map partition_rows{dht::ring_position_less_comparator(*schema)};
        std::vector<mutation> muts;
        auto pk = 0;
        for (auto partition_size_100kb : partition_sizes_100kb) {
            auto mut_desc = tests::data_model::mutation_description(pkeys.at(pk++).key().explode(*schema));
            for (auto ck = 0; ck < partition_size_100kb; ++ck) {
                mut_desc.add_clustered_cell({int32_type->decompose(data_value(ck))}, "v", tests::data_model::mutation_description::value(blob_100kb));
            }
            muts.push_back(mut_desc.build(schema));
            partition_rows.emplace(muts.back().decorated_key(), partition_size_100kb);
        }

        boost::sort(muts, [less = dht::ring_position_less_comparator(*schema)] (const mutation& a, const mutation& b) {
            return less(a.decorated_key(), b.decorated_key());
        });

        auto mt = make_lw_shared<memtable>(schema);
        for (const auto& mut : muts) {
            mt->apply(mut);
        }

        auto staging_reader = mt->as_data_source().make_reader(
                schema,
                no_reader_permit(),
                query::full_partition_range,
                schema->full_slice(),
                service::get_local_streaming_read_priority(),
                nullptr,
                streamed_mutation::forwarding::no,
                ::mutation_reader::forwarding::no);

        std::vector<mutation> collected_muts;
        bool failed = false;

        staging_reader.consume_in_thread(db::view::view_updating_consumer(schema, as,
                    consumer_verifier(schema, partition_rows, collected_muts, failed)), db::no_timeout);

        BOOST_REQUIRE(!failed);

        BOOST_REQUIRE_EQUAL(muts.size(), collected_muts.size());
        for (size_t i = 0; i < muts.size(); ++i) {
            BOOST_TEST_MESSAGE(fmt::format("compare mutation {}", i));
            BOOST_REQUIRE_EQUAL(muts[i], collected_muts[i]);
        }
    }
}
