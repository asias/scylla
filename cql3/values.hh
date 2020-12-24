/*
 * Copyright (C) 2017 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include "types.hh"
#include "bytes.hh"

#include <optional>
#include <variant>

#include <seastar/util/variant_utils.hh>

#include "utils/fragmented_temporary_buffer.hh"

namespace cql3 {

struct null_value {
};

struct unset_value {
};

/// \brief View to a raw CQL protocol value.
///
/// \see raw_value
struct raw_value_view {
    std::variant<fragmented_temporary_buffer::view, null_value, unset_value> _data;

    raw_value_view(null_value&& data)
        : _data{std::move(data)}
    {}
    raw_value_view(unset_value&& data)
        : _data{std::move(data)}
    {}
    raw_value_view(fragmented_temporary_buffer::view data)
        : _data{data}
    {}
public:
    static raw_value_view make_null() {
        return raw_value_view{std::move(null_value{})};
    }
    static raw_value_view make_unset_value() {
        return raw_value_view{std::move(unset_value{})};
    }
    static raw_value_view make_value(fragmented_temporary_buffer::view view) {
        return raw_value_view{view};
    }
    bool is_null() const {
        return std::holds_alternative<null_value>(_data);
    }
    bool is_unset_value() const {
        return std::holds_alternative<unset_value>(_data);
    }
    bool is_value() const {
        return std::holds_alternative<fragmented_temporary_buffer::view>(_data);
    }
    std::optional<fragmented_temporary_buffer::view> data() const {
        if (auto pdata = std::get_if<fragmented_temporary_buffer::view>(&_data)) {
            return *pdata;
        }
        return {};
    }
    explicit operator bool() const {
        return is_value();
    }
    const fragmented_temporary_buffer::view* operator->() const {
        return &std::get<fragmented_temporary_buffer::view>(_data);
    }
    const fragmented_temporary_buffer::view& operator*() const {
        return std::get<fragmented_temporary_buffer::view>(_data);
    }

    bool operator==(const raw_value_view& other) const {
        if (_data.index() != other._data.index()) {
            return false;
        }
        if (is_value() && **this != *other) {
            return false;
        }
        return true;
    }
    bool operator!=(const raw_value_view& other) const {
        return !(*this == other);
    }

    friend std::ostream& operator<<(std::ostream& os, const raw_value_view& value) {
        seastar::visit(value._data, [&] (fragmented_temporary_buffer::view v) {
            os << "{ value: ";
            using boost::range::for_each;
            for_each(v, [&os] (bytes_view bv) { os << bv; });
            os << " }";
        }, [&] (null_value) {
            os << "{ null }";
        }, [&] (unset_value) {
            os << "{ unset }";
        });
        return os;
    }
};

/// \brief Raw CQL protocol value.
///
/// The `raw_value` type represents an uninterpreted value from the CQL wire
/// protocol. A raw value can hold either a null value, an unset value, or a byte
/// blob that represents the value.
class raw_value {
    std::variant<bytes, null_value, unset_value> _data;

    raw_value(null_value&& data)
        : _data{std::move(data)}
    {}
    raw_value(unset_value&& data)
        : _data{std::move(data)}
    {}
    raw_value(bytes&& data)
        : _data{std::move(data)}
    {}
    raw_value(const bytes& data)
        : _data{data}
    {}
public:
    static raw_value make_null() {
        return raw_value{std::move(null_value{})};
    }
    static raw_value make_unset_value() {
        return raw_value{std::move(unset_value{})};
    }
    static raw_value make_value(const raw_value_view& view) {
        if (view.is_null()) {
            return make_null();
        }
        if (view.is_unset_value()) {
            return make_unset_value();
        }
        return make_value(linearized(*view));
    }
    static raw_value make_value(bytes&& bytes) {
        return raw_value{std::move(bytes)};
    }
    static raw_value make_value(const bytes& bytes) {
        return raw_value{bytes};
    }
    static raw_value make_value(const bytes_opt& bytes) {
        if (bytes) {
            return make_value(*bytes);
        }
        return make_null();
    }
    bool is_null() const {
        return std::holds_alternative<null_value>(_data);
    }
    bool is_unset_value() const {
        return std::holds_alternative<unset_value>(_data);
    }
    bool is_value() const {
        return std::holds_alternative<bytes>(_data);
    }
    bytes_opt data() const {
        if (auto pdata = std::get_if<bytes>(&_data)) {
            return *pdata;
        }
        return {};
    }
    explicit operator bool() const {
        return is_value();
    }
    const bytes* operator->() const {
        return &std::get<bytes>(_data);
    }
    const bytes& operator*() const {
        return std::get<bytes>(_data);
    }
    raw_value_view to_view() const {
        switch (_data.index()) {
        case 0:  return raw_value_view::make_value(fragmented_temporary_buffer::view(bytes_view{std::get<bytes>(_data)}));
        case 1:  return raw_value_view::make_null();
        default: return raw_value_view::make_unset_value();
        }
    }
};

}

inline bytes to_bytes(const cql3::raw_value_view& view)
{
    return linearized(*view);
}

inline bytes_opt to_bytes_opt(const cql3::raw_value_view& view) {
    auto buffer_view = view.data();
    if (buffer_view) {
        return bytes_opt(linearized(*buffer_view));
    }
    return bytes_opt();
}

inline bytes_opt to_bytes_opt(const cql3::raw_value& value) {
    return to_bytes_opt(value.to_view());
}
