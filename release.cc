/*
 * Copyright (C) 2015 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#include "version.hh"

#include <seastar/core/print.hh>

static const char scylla_version_str[] = SCYLLA_VERSION;
static const char scylla_release_str[] = SCYLLA_RELEASE;

std::string scylla_version()
{
    return format("{}-{}", scylla_version_str, scylla_release_str);
}

// get the version number into writeable memory, so we can grep for it if we get a core dump
std::string version_stamp_for_core
    = "VERSION VERSION VERSION $Id: " + scylla_version() + " $ VERSION VERSION VERSION";
