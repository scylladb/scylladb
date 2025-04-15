/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once
#include "schema/schema_fwd.hh"

class mutation_reader;
class reader_permit;

mutation_reader make_empty_flat_reader_v2(schema_ptr s, reader_permit permit);

