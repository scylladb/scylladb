/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once
#include "schema_fwd.hh"

class flat_mutation_reader;
class reader_permit;

flat_mutation_reader make_empty_flat_reader(schema_ptr s, reader_permit permit);

