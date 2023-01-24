/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <utility>

class scylla_tests_cmdline_options_processor {
private:
    int _new_argc = 0;
    char** _new_argv = nullptr;
public:
    scylla_tests_cmdline_options_processor() = default;
    ~scylla_tests_cmdline_options_processor();
    // Returns new argv if compaction group option was processed.
    std::pair<int, char**> process_cmdline_options(int argc, char** argv);
};
