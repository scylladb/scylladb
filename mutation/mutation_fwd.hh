/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "utils/chunked_vector.hh"

class mutation;
class frozen_mutation;
class canonical_mutation;

using mutation_vector = utils::chunked_vector<mutation>;
using frozen_mutation_vector = utils::chunked_vector<frozen_mutation>;
using canonical_mutation_vector = utils::chunked_vector<canonical_mutation>;
