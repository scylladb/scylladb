/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

class flat_mutation_reader;

flat_mutation_reader make_forwardable(flat_mutation_reader m);

