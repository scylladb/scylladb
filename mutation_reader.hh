/*
 * Copyright 2015 Cloudius Systems
 */

#pragma once

#include <vector>

#include "mutation.hh"
#include "core/future.hh"

// A mutation_reader is an object which allows iterating on mutations: invoke
// the function to get a future for the next mutation, with an unset optional
// marking the end of iteration. After calling mutation_reader's operator(),
// caller must keep the object alive until the returned future is fulfilled.
// TODO: When iterating over mutations, we don't need a schema_ptr for every
// single one as it is normally the same for all of them. So "mutation" might
// not be the optimal object to use here.
using mutation_reader = std::function<future<mutation_opt>()>;
