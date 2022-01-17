/*
 * Copyright 2016-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

class frozen_mutation final {
    bytes representation();
};

class frozen_mutation_fragment final {
    bytes representation();
};
