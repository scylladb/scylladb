/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once
#include "readers/mutation_reader_fwd.hh"

mutation_reader make_delegating_reader(mutation_reader&);
