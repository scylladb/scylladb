/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

class flat_mutation_reader;
class flat_mutation_reader_v2;

// Adapts a v2 reader to v1 reader
flat_mutation_reader downgrade_to_v1(flat_mutation_reader_v2);

// Adapts a v1 reader to v2 reader
flat_mutation_reader_v2 upgrade_to_v2(flat_mutation_reader);
