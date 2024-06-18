/*
 * Copyright (C) 2019-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <seastar/util/noncopyable_function.hh>

#include "readers/mutation_reader.hh"

namespace mutation_writer {

using classify_by_timestamp = noncopyable_function<int64_t(api::timestamp_type)>;
future<> segregate_by_timestamp(mutation_reader producer, classify_by_timestamp classifier, reader_consumer_v2 consumer);

} // namespace mutation_writer
