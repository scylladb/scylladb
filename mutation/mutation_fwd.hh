/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <vector>

class mutation;
class frozen_mutation;
class canonical_mutation;

using mutation_vector = std::vector<mutation>;
using frozen_mutation_vector = std::vector<frozen_mutation>;
using canonical_mutation_vector = std::vector<canonical_mutation>;
