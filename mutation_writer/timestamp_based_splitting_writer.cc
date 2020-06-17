/*
 * Copyright (C) 2019 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#include "mutation_writer/timestamp_based_splitting_writer.hh"

#include <cinttypes>
#include <boost/range/adaptor/transformed.hpp>
#include <boost/range/algorithm/min_element.hpp>
#include <seastar/core/shared_mutex.hh>

#include "mutation_reader.hh"

namespace mutation_writer {

namespace {

// TODO: should probably move to utils and make it a proper container.
template <typename Key, typename Value, size_t Size>
class small_flat_map {
public:
    // The underlying flat container requires value_type to be move
    // assignable, so we can't have a const key.
    using key_type = Key;
    using mapped_type = Value;
    using value_type = std::pair<Key, Value>;
    using container = utils::small_vector<value_type, Size>;
    using iterator = typename container::iterator;
private:
    container _values;

public:
    // Element access
    mapped_type& at(const key_type& k);
    mapped_type& operator[](const key_type& k);

    // Iterators
    iterator begin() { return _values.begin(); }
    iterator end() { return _values.end(); }

    // Modifiers
    template <typename... Args>
    std::pair<iterator, bool> emplace(Args&&... args);

    // Lookup
    iterator find(const key_type& k);
};

template <typename Key, typename Value, size_t Size>
typename small_flat_map<Key, Value, Size>::mapped_type&
small_flat_map<Key, Value, Size>::at(const key_type& k) {
    if (auto it = find(k); it != end()) {
        return it->second;
    }
    throw std::out_of_range();
}

template <typename Key, typename Value, size_t Size>
typename small_flat_map<Key, Value, Size>::mapped_type&
small_flat_map<Key, Value, Size>::operator[](const key_type& k) {
    if (auto it = find(k); it != end()) {
        return it->second;
    }
    _values.emplace_back(k, mapped_type{});
    return _values.back().second;
}

template <typename Key, typename Value, size_t Size>
template <typename... Args>
std::pair<typename small_flat_map<Key, Value, Size>::iterator, bool>
small_flat_map<Key, Value, Size>::emplace(Args&&... args) {
    value_type elem(std::forward<Args>(args)...);
    if (auto it = find(elem.first); it != end()) {
        return std::pair(it, false);
    }
    _values.emplace_back(std::move(elem));
    return std::pair(end() - 1, true);
}

template <typename Key, typename Value, size_t Size>
typename small_flat_map<Key, Value, Size>::iterator
small_flat_map<Key, Value, Size>::find(const key_type& k) {
    auto key_matches = [&k] (const value_type& v) { return v.first == k; };
    if (auto it = std::find_if(begin(), end(), key_matches); it != _values.end()) {
        return it;
    }
    return end();
}

} // anonymous namespace

class timestamp_based_splitting_mutation_writer {
    using bucket_id = int64_t;

    class bucket_writer {
        schema_ptr _schema;
        queue_reader_handle _handle;
        future<> _consume_fut;
        bool _has_current_partition = false;

    private:
        bucket_writer(schema_ptr schema, std::pair<flat_mutation_reader, queue_reader_handle> queue_reader, reader_consumer& consumer)
            : _schema(std::move(schema))
            , _handle(std::move(queue_reader.second))
            , _consume_fut(consumer(std::move(queue_reader.first))) {
        }

    public:
        bucket_writer(schema_ptr schema, reader_consumer& consumer)
            : bucket_writer(schema, make_queue_reader(schema), consumer) {
        }
        void set_has_current_partition() {
            _has_current_partition = true;
        }
        void clear_has_current_partition() {
            _has_current_partition = false;
        }
        bool has_current_partition() const {
            return _has_current_partition;
        }
        future<> consume(mutation_fragment mf) {
            return _handle.push(std::move(mf));
        }
        future<> consume_end_of_stream() {
            _handle.push_end_of_stream();
            return std::move(_consume_fut);
        }
    };

private:
    schema_ptr _schema;
    classify_by_timestamp _classifier;
    reader_consumer _consumer;
    partition_start _current_partition_start;
    std::unordered_map<bucket_id, bucket_writer> _buckets;
    std::vector<bucket_id> _buckets_used_for_current_partition;

private:
    future<> write_to_bucket(bucket_id bucket, mutation_fragment&& mf);

    std::optional<bucket_id> examine_column(const atomic_cell_or_collection& c, const column_definition& cdef);
    std::optional<bucket_id> examine_row(const row& r, column_kind kind);
    std::optional<bucket_id> examine_static_row(const static_row& sr);
    std::optional<bucket_id> examine_clustering_row(const clustering_row& cr);
    small_flat_map<bucket_id, atomic_cell_or_collection, 4> split_collection(atomic_cell_or_collection&& collection, const column_definition& cdef);
    small_flat_map<bucket_id, row, 4> split_row(column_kind kind, row&& r);
    small_flat_map<bucket_id, static_row, 4> split_static_row(static_row&& sr);
    small_flat_map<bucket_id, clustering_row, 4> split_clustering_row(clustering_row&& cr);
    future<> write_marker_and_tombstone(const clustering_row& cr);

public:
    timestamp_based_splitting_mutation_writer(schema_ptr schema, classify_by_timestamp classifier, reader_consumer consumer)
        : _schema(std::move(schema))
        , _classifier(std::move(classifier))
        , _consumer(std::move(consumer))
        , _current_partition_start(dht::decorated_key(dht::token{}, partition_key::make_empty()), tombstone{}) {
    }

    future<> consume(partition_start&& ps);
    future<> consume(static_row&& sr);
    future<> consume(clustering_row&& cr);
    future<> consume(range_tombstone&& rt);
    future<> consume(partition_end&& pe);

    future<> consume_end_of_stream() {
        return parallel_for_each(_buckets, [] (std::pair<const bucket_id, bucket_writer>& bucket) {
            return bucket.second.consume_end_of_stream();
        });
    }
};

future<> timestamp_based_splitting_mutation_writer::write_to_bucket(bucket_id bucket, mutation_fragment&& mf) {
    auto it = _buckets.find(bucket);
    if (it == _buckets.end()) {
        std::tie(it, std::ignore) = _buckets.emplace(bucket, bucket_writer(_schema, _consumer));
    }

    auto& writer = it->second;

    if (writer.has_current_partition()) {
        return writer.consume(std::move(mf));
    }

    // We can explicitly write a partition-start fragment when the partition has
    // a partition tombstone.
    if (mf.is_partition_start()) {
        return writer.consume(std::move(mf)).then([this, bucket = it->first, &writer] {
            writer.set_has_current_partition();
            _buckets_used_for_current_partition.push_back(bucket);
        });
    }

    return writer.consume(partition_start(_current_partition_start)).then([this, bucket = it->first, &writer, mf = std::move(mf)] () mutable {
        writer.set_has_current_partition();
        _buckets_used_for_current_partition.push_back(bucket);
        return writer.consume(std::move(mf));
    });
}

std::optional<timestamp_based_splitting_mutation_writer::bucket_id> timestamp_based_splitting_mutation_writer::examine_column(
        const atomic_cell_or_collection& cell, const column_definition& cdef) {
    if (cdef.is_atomic()) {
        return _classifier(cell.as_atomic_cell(cdef).timestamp());
    }
    if (cdef.type->is_collection() || cdef.type->is_user_type()) {
        std::optional<bucket_id> bucket;
        bool mismatch = false;
        cell.as_collection_mutation().with_deserialized(*cdef.type, [&, this] (collection_mutation_view_description mv) {
            if (mv.tomb) {
                bucket = _classifier(mv.tomb.timestamp);
            }
            for (auto&& c : mv.cells) {
                if (mismatch) {
                    break;
                }
                const auto this_bucket = _classifier(c.second.timestamp());
                mismatch |= bucket.value_or(this_bucket) != this_bucket;
                bucket = this_bucket;
            }
        });
        if (mismatch) {
            bucket.reset();
        }
        return bucket;
    }
    throw std::runtime_error(fmt::format("Cannot classify timestamp of cell (column {} of uknown type {})", cdef.name_as_text(), cdef.type->name()));
}

std::optional<timestamp_based_splitting_mutation_writer::bucket_id> timestamp_based_splitting_mutation_writer::examine_row(const row& r,
        column_kind kind) {
    std::optional<bucket_id> bucket;
    r.for_each_cell_until([this, &bucket, kind] (column_id id, const atomic_cell_or_collection& cell) {
        const auto this_bucket = examine_column(cell, _schema->column_at(kind, id));
        if (!this_bucket || (bucket && *bucket != *this_bucket)) {
            bucket.reset();
            return stop_iteration::yes;
        }
        bucket = this_bucket;
        return stop_iteration::no;
    });
    return bucket;
}

std::optional<timestamp_based_splitting_mutation_writer::bucket_id> timestamp_based_splitting_mutation_writer::examine_static_row(
        const static_row& sr) {
    return examine_row(sr.cells(), column_kind::static_column);
}

std::optional<timestamp_based_splitting_mutation_writer::bucket_id> timestamp_based_splitting_mutation_writer::examine_clustering_row(
        const clustering_row& cr) {
    std::optional<bucket_id> bucket_id;

    if (!cr.marker().is_missing()) {
        auto marker_bucket_id = _classifier(cr.marker().timestamp());
        if (bucket_id) {
            if (*bucket_id != marker_bucket_id) {
                return {};
            }
        } else {
            bucket_id = marker_bucket_id;
        }
    }
    if (cr.tomb() != row_tombstone{}) {
        auto tomb_bucket_id = _classifier(cr.tomb().tomb().timestamp);
        if (bucket_id) {
            if (*bucket_id != tomb_bucket_id) {
                return {};
            }
        } else {
            bucket_id = tomb_bucket_id;
        }
    }

    const auto cells_bucket_id = examine_row(cr.cells(), column_kind::regular_column);
    if (!cells_bucket_id) {
        return {};
    }
    if (bucket_id) {
        if (*bucket_id != *cells_bucket_id) {
            return {};
        }
    } else {
        bucket_id = cells_bucket_id;
    }
    return bucket_id;
}

small_flat_map<timestamp_based_splitting_mutation_writer::bucket_id, atomic_cell_or_collection, 4>
timestamp_based_splitting_mutation_writer::split_collection(atomic_cell_or_collection&& collection, const column_definition& cdef) {
    small_flat_map<bucket_id, atomic_cell_or_collection, 4> pieces_by_bucket;

    collection.as_collection_mutation().with_deserialized(*cdef.type, [&, this] (collection_mutation_view_description original_mv) {
        small_flat_map<bucket_id, collection_mutation_view_description, 4> mutations_by_bucket;
        for (auto&& c : original_mv.cells) {
            mutations_by_bucket[_classifier(c.second.timestamp())].cells.push_back(c);
        }
        if (original_mv.tomb) {
            mutations_by_bucket[_classifier(original_mv.tomb.timestamp)].tomb = original_mv.tomb;
        }

        for (auto&& [bucket, bucket_mv] : mutations_by_bucket) {
            pieces_by_bucket.emplace(bucket, bucket_mv.serialize(*cdef.type));
        }
    });

    return pieces_by_bucket;
}

small_flat_map<timestamp_based_splitting_mutation_writer::bucket_id, row, 4>
timestamp_based_splitting_mutation_writer::split_row(column_kind kind, row&& r) {
    small_flat_map<bucket_id, row, 4> rows_by_bucket;

    r.for_each_cell([&, this, kind] (column_id id, atomic_cell_or_collection& cell) {
        const auto& cdef = _schema->column_at(kind, id);
        if (cdef.type->is_atomic()) {
            rows_by_bucket[_classifier(cell.as_atomic_cell(cdef).timestamp())].append_cell(id, std::move(cell));
        } else if (cdef.type->is_collection() || cdef.type->is_user_type()) {
            for (auto&& [bucket, cell_piece] : split_collection(std::move(cell), cdef)) {
                rows_by_bucket[bucket].append_cell(id, std::move(cell_piece));
            }
        } else {
            throw std::runtime_error(fmt::format("Cannot classify cell {} of unknown type {}", cdef.name_as_text(), cdef.type->name()));
        }
    });

    return rows_by_bucket;
}

small_flat_map<timestamp_based_splitting_mutation_writer::bucket_id, static_row, 4>
timestamp_based_splitting_mutation_writer::split_static_row(static_row&& sr) {
    small_flat_map<bucket_id, static_row, 4> static_rows_by_bucket;

    for (auto&& [bucket_id, row] : split_row(column_kind::static_column, std::move(sr.cells()))) {
        static_rows_by_bucket.emplace(bucket_id, static_row(std::move(row)));
    }

    return static_rows_by_bucket;
}

small_flat_map<timestamp_based_splitting_mutation_writer::bucket_id, clustering_row, 4>
timestamp_based_splitting_mutation_writer::split_clustering_row(clustering_row&& cr) {
    small_flat_map<bucket_id, clustering_row, 4> clustering_rows_by_bucket;

    for (auto&& [bucket_id, row] : split_row(column_kind::regular_column, std::move(cr.cells()))) {
        clustering_rows_by_bucket.emplace(bucket_id, clustering_row(cr.key(), {}, {}, std::move(row)));
    }

    if (!cr.marker().is_missing()) {
        const auto marker_bucket_id = _classifier(cr.marker().timestamp());
        if (auto it = clustering_rows_by_bucket.find(marker_bucket_id); it != clustering_rows_by_bucket.end()) {
            it->second.apply(cr.marker());
        } else {
            clustering_rows_by_bucket.emplace(marker_bucket_id, clustering_row(cr.key(), {}, cr.marker(), {}));
        }
    }

    if (cr.tomb() != row_tombstone{}) {
        const auto tomb_bucket_id = _classifier(cr.tomb().tomb().timestamp);
        if (auto it = clustering_rows_by_bucket.find(tomb_bucket_id); it != clustering_rows_by_bucket.end()) {
            it->second.apply(cr.tomb().regular());
            it->second.apply(cr.tomb().shadowable());
        } else {
            clustering_rows_by_bucket.emplace(tomb_bucket_id, clustering_row(cr.key(), cr.tomb(), {}, {}));
        }
    }

    return clustering_rows_by_bucket;
}

future<> timestamp_based_splitting_mutation_writer::write_marker_and_tombstone(const clustering_row& cr) {
    auto marker_bucket_id = cr.marker().is_missing() ? std::optional<int64_t>{} : std::optional<int64_t>{_classifier(cr.marker().timestamp())};
    auto tomb_bucket_id = cr.tomb() == row_tombstone{} ? std::optional<int64_t>{} : std::optional<int64_t>{_classifier(cr.tomb().tomb().timestamp)};
    if (!marker_bucket_id && !tomb_bucket_id) {
        return make_ready_future<>();
    }

    if (marker_bucket_id == tomb_bucket_id) {
        return write_to_bucket(*marker_bucket_id, clustering_row(cr.key(), cr.tomb(), cr.marker(), {}));
    }

    auto write_marker_fut = make_ready_future<>();
    if (marker_bucket_id) {
        write_marker_fut = write_to_bucket(*marker_bucket_id, clustering_row(cr.key(), {}, cr.marker(), {}));
    }

    auto write_tomb_fut = make_ready_future<>();
    if (tomb_bucket_id) {
        write_tomb_fut = write_to_bucket(*tomb_bucket_id, clustering_row(cr.key(), cr.tomb(), {}, {}));
    }
    return when_all_succeed(std::move(write_marker_fut), std::move(write_tomb_fut));
}

future<> timestamp_based_splitting_mutation_writer::consume(partition_start&& ps) {
    _current_partition_start = std::move(ps);
    if (auto& tomb = _current_partition_start.partition_tombstone()) {
        auto bucket = _classifier(tomb.timestamp);
        auto ps = partition_start(_current_partition_start);
        tomb = {};
        return write_to_bucket(bucket, std::move(ps));
    }
    return make_ready_future<>();
}

future<> timestamp_based_splitting_mutation_writer::consume(static_row&& sr) {
    if (sr.cells().empty()) {
        return make_ready_future<>();
    }

    if (const auto bucket = examine_static_row(sr)) {
        return write_to_bucket(*bucket, std::move(sr));
    }

    return parallel_for_each(split_static_row(std::move(sr)), [this] (std::pair<bucket_id, static_row>& sr_piece) {
        return write_to_bucket(sr_piece.first, static_row(std::move(sr_piece.second)));
    });
}

future<> timestamp_based_splitting_mutation_writer::consume(clustering_row&& cr) {
    if (cr.cells().empty()) {
        return write_marker_and_tombstone(cr);
    }

    if (const auto bucket = examine_clustering_row(cr)) {
        return write_to_bucket(*bucket, std::move(cr));
    }

    return parallel_for_each(split_clustering_row(std::move(cr)), [this] (std::pair<bucket_id, clustering_row>& cr_piece) {
        return write_to_bucket(cr_piece.first, std::move(cr_piece.second));
    });
}

future<> timestamp_based_splitting_mutation_writer::consume(range_tombstone&& rt) {
    return write_to_bucket(_classifier(rt.tomb.timestamp), std::move(rt));
}

future<> timestamp_based_splitting_mutation_writer::consume(partition_end&& pe) {
    return parallel_for_each(_buckets_used_for_current_partition, [this, pe = std::move(pe)] (bucket_id bucket) {
        auto& writer = _buckets.at(bucket);
        return writer.consume(mutation_fragment(partition_end(pe))).then([&writer] {
            writer.clear_has_current_partition();
        });
    }).then([this] {
        _buckets_used_for_current_partition.clear();
    });
}

future<> segregate_by_timestamp(flat_mutation_reader producer, classify_by_timestamp classifier, reader_consumer consumer) {
    //FIXME: make this into a consume() variant?
    auto schema = producer.schema();
    return do_with(
            std::move(producer),
            timestamp_based_splitting_mutation_writer(std::move(schema), std::move(classifier), std::move(consumer)),
            [] (flat_mutation_reader& rd, timestamp_based_splitting_mutation_writer& wr) {
        return rd.fill_buffer(db::no_timeout).then([&rd, &wr] {
            return do_until([&rd] { return rd.is_buffer_empty() && rd.is_end_of_stream(); }, [&rd, &wr] {
                auto f1 = rd.pop_mutation_fragment().consume(wr);
                auto f2 = rd.is_buffer_empty() ? rd.fill_buffer(db::no_timeout) : make_ready_future<>();
                return when_all_succeed(std::move(f1), std::move(f2));
            });
        }).finally([&wr] {
            return wr.consume_end_of_stream();
        });
    });
}

} // namespace mutation_writer
