/*
 * Copyright (C) 2014 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include <memory>
#include <ostream>
#include <core/sstring.hh>
#include <core/future.hh>

#include "seastarx.hh"

namespace utils {
    class file_lock {
    public:
        file_lock() = delete;
        file_lock(const file_lock&) = delete;
        file_lock(file_lock&&) noexcept;
        ~file_lock();

        file_lock& operator=(file_lock&&) = default;

        static future<file_lock> acquire(sstring);

        sstring path() const;
        sstring to_string() const {
            return path();
        }
    private:
        class impl;
        file_lock(sstring);
        std::unique_ptr<impl> _impl;
    };

    std::ostream& operator<<(std::ostream& out, const file_lock& f);
}

