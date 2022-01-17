/*
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */
#pragma once
#include <seastar/util/log.hh>

// A test log to use in all unit tests, including boost unit
// tests. Built-in boost logging log levels do not allow to filter
// out unimportant messages, which then clutter xunit-format XML
// output, so are not used for anything profuse.

extern seastar::logger testlog;

