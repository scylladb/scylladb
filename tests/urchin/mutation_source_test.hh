/*
 * Copyright 2015 Cloudius Systems
 */

#pragma once

#include "mutation_reader.hh"

using populate_fn = std::function<mutation_source(schema_ptr s, const std::vector<mutation>&)>;

// Must be run in a seastar thread
void run_mutation_source_tests(populate_fn populate);
