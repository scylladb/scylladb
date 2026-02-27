/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

class mutation_reader;

mutation_reader make_nonforwardable(mutation_reader, bool);
