/*
 * Copyright 2016-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

class commitlog_entry [[writable]] {
    std::optional<column_mapping> mapping();
    frozen_mutation mutation();
};
