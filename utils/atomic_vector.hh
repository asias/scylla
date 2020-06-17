/*
 * Copyright (C) 2020 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include <seastar/core/rwlock.hh>
#include <seastar/util/defer.hh>
#include <seastar/util/noncopyable_function.hh>

#include <vector>

// This class supports atomic removes (by using a lock and returning a
// future) and non atomic insert and iteration (by using indexes).
template <typename T>
class atomic_vector {
    std::vector<T> _vec;
    seastar::rwlock _vec_lock;

public:
    void add(const T& value) {
        _vec.push_back(value);
    }
    seastar::future<> remove(const T& value) {
        return with_lock(_vec_lock.for_write(), [this, value] {
            _vec.erase(std::remove(_vec.begin(), _vec.end(), value), _vec.end());
        });
    }

    // This must be called on a thread. The callback function must not
    // call remove.
    void for_each(seastar::noncopyable_function<void(T&)> func) {
        _vec_lock.for_read().lock().get();
        auto unlock = seastar::defer([this] {
            _vec_lock.for_read().unlock();
        });
        // We grab a lock in remove(), but not in add(), so we
        // iterate using indexes to guard against the vector being
        // reallocated.
        for (size_t i = 0, n = _vec.size(); i < n; ++i) {
            func(_vec[i]);
        }
    }
};
