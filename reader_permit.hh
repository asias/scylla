/*
 * Copyright (C) 2019 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include <seastar/core/shared_ptr.hh>
#include <seastar/core/file.hh>
#include "seastarx.hh"

struct reader_resources {
    int count = 0;
    ssize_t memory = 0;

    reader_resources() = default;

    reader_resources(int count, ssize_t memory)
        : count(count)
        , memory(memory) {
    }

    bool operator>=(const reader_resources& other) const {
        return count >= other.count && memory >= other.memory;
    }

    reader_resources& operator-=(const reader_resources& other) {
        count -= other.count;
        memory -= other.memory;
        return *this;
    }

    reader_resources& operator+=(const reader_resources& other) {
        count += other.count;
        memory += other.memory;
        return *this;
    }

    explicit operator bool() const {
        return count >= 0 && memory >= 0;
    }
};

class reader_concurrency_semaphore;

class reader_permit {
    struct impl {
        reader_concurrency_semaphore& semaphore;
        reader_resources base_cost;

        impl(reader_concurrency_semaphore& semaphore, reader_resources base_cost);
        ~impl();
    };

    friend reader_permit no_reader_permit();

public:
    class memory_units {
        reader_concurrency_semaphore* _semaphore = nullptr;
        size_t _memory = 0;

        friend class reader_permit;
    private:
        memory_units(reader_concurrency_semaphore* semaphore, ssize_t memory) noexcept;
    public:
        memory_units(const memory_units&) = delete;
        memory_units(memory_units&&) noexcept;
        ~memory_units();
        memory_units& operator=(const memory_units&) = delete;
        memory_units& operator=(memory_units&&) noexcept;
        void reset(size_t memory = 0);
        operator size_t() const {
            return _memory;
        }
    };

private:
    lw_shared_ptr<impl> _impl;

private:
    reader_permit() = default;

public:
    reader_permit(reader_concurrency_semaphore& semaphore, reader_resources base_cost);

    bool operator==(const reader_permit& o) const {
        return _impl == o._impl;
    }
    operator bool() const {
        return bool(_impl);
    }

    memory_units get_memory_units(size_t memory = 0);
    void release();
};

reader_permit no_reader_permit();

template <typename Char>
temporary_buffer<Char> make_tracked_temporary_buffer(temporary_buffer<Char> buf, reader_permit& permit) {
    return temporary_buffer<Char>(buf.get_write(), buf.size(),
            make_deleter(buf.release(), [units = permit.get_memory_units(buf.size())] () mutable { units.reset(); }));
}

file make_tracked_file(file f, reader_permit p);
