/*
 * Copyright 2018-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <seastar/core/seastar.hh>

namespace db {
    class commitlog_file_extension {
    public:
        virtual ~commitlog_file_extension() {}
        virtual seastar::future<seastar::file> wrap_file(const seastar::sstring& filename,
            seastar::file, seastar::open_flags flags) = 0;
        virtual seastar::future<> before_delete(const seastar::sstring& filename) = 0;
    };
}

