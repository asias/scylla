/*
 * Copyright 2015 ScyllaDB
 */

/* This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#include <seastar/core/print.hh>
#include <seastar/util/log.hh>
#include <seastar/util/backtrace.hh>

#include <exception>
#include <system_error>
#include <atomic>
#include "exceptions.hh"

#include <iostream>

bool check_exception(system_error_lambda_t f)
{
    auto e = std::current_exception();
    if (!e) {
        return false;
    }

    try {
        std::rethrow_exception(e);
    } catch (std::system_error &e) {
        return f(e);
    } catch (...) {
        return false;
    }
    return false;
}

bool is_system_error_errno(int err_no)
{
    return check_exception([err_no] (const std::system_error &e) {
        auto code = e.code();
        return code.value() == err_no &&
               code.category() == std::system_category();
    });
}

bool should_stop_on_system_error(const std::system_error& e) {
    if (e.code().category() == std::system_category()) {
	// Whitelist of errors that don't require us to stop the server:
	switch (e.code().value()) {
        case EEXIST:
        case ENOENT:
            return false;
        default:
            break;
        }
    }
    return true;
}

std::atomic<bool> abort_on_internal_error{false};

void set_abort_on_internal_error(bool do_abort) {
    abort_on_internal_error.store(do_abort);
}

void on_internal_error(seastar::logger& logger, const seastar::sstring& msg) {
    if (abort_on_internal_error.load()) {
        logger.error("{}, at: {}", msg.c_str(), seastar::current_backtrace());
        abort();
    } else {
        seastar::throw_with_backtrace<std::runtime_error>(msg.c_str());
    }
}
