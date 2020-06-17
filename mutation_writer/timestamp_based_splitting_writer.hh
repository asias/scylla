/*
 * Copyright (C) 2019 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include <seastar/util/noncopyable_function.hh>

#include "flat_mutation_reader.hh"

namespace mutation_writer {

using classify_by_timestamp = noncopyable_function<int64_t(api::timestamp_type)>;
using reader_consumer = noncopyable_function<future<> (flat_mutation_reader)>;

future<> segregate_by_timestamp(flat_mutation_reader producer, classify_by_timestamp classifier, reader_consumer consumer);

} // namespace mutation_writer
