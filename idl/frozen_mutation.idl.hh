/*
 * Copyright 2016-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

class frozen_mutation final {
    bytes representation();
};

class frozen_mutation_fragment final {
    bytes representation();
};
