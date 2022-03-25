/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include <boost/range/algorithm/heap_algorithm.hpp>
#include <boost/range/algorithm/reverse.hpp>
#include <boost/move/iterator.hpp>
#include <variant>

#include <seastar/core/future-util.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/util/closeable.hh>

#include "mutation_reader.hh"
#include "readers/flat_mutation_reader.hh"
#include "readers/empty.hh"
#include "schema_registry.hh"
#include "mutation_compactor.hh"
#include "dht/sharder.hh"
#include "readers/empty_v2.hh"
#include "readers/combined.hh"

logging::logger mrlog("mutation_reader");


