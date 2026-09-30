/*
 * Copyright 2018-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
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

        /// Called before segment \p from is renamed to \p to (possibly in another directory).
        /// Any files the extension keeps for the segment must be persisted under the new name
        /// when this returns, so that the segment is never left without them, even after a crash.
        /// The files for the old name must be kept: the rename may still fail.
        virtual seastar::future<> before_rename(const seastar::sstring& from, const seastar::sstring& to) {
            return seastar::make_ready_future<>();
        }
        /// Called after segment \p from has been renamed to \p to and the rename has been persisted.
        /// Removes the files the extension kept for the old name.
        virtual seastar::future<> after_rename(const seastar::sstring& from, const seastar::sstring& to) {
            return seastar::make_ready_future<>();
        }
    };
}

