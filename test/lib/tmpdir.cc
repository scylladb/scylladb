/*
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "test/lib/tmpdir.hh"
#include "utils/UUID.hh"

#include <seastar/util/alloc_failure_injector.hh>

// This is not really noexcept. But it is used only from the
// destructor and move assignment operators which have to be
// noexcept. This is only for testing, so a std::unexpected call if
// remove fails is fine.
void tmpdir::remove() noexcept {
    memory::scoped_critical_alloc_section dfg;
    if (!_path.empty()) {
        fs::remove_all(_path);
    }
}

tmpdir::sweeper::~sweeper() {
    memory::scoped_critical_alloc_section dfg;
    if (!_tmpd._path.empty()) {
        for (const auto& ent : fs::directory_iterator(_tmpd._path)) {
            fs::remove_all(ent.path());
        }
    }
}

tmpdir::tmpdir()
    : _path(fs::temp_directory_path() / fs::path(fmt::format(FMT_STRING("scylla-{}"), utils::make_random_uuid()))) {
    fs::create_directories(_path);
}

tmpdir::tmpdir(tmpdir&& other) noexcept : _path(std::exchange(other._path, {})) {}

void tmpdir::operator=(tmpdir&& other) noexcept {
    remove();
    _path = std::exchange(other._path, {});
}

tmpdir::~tmpdir() {
    remove();
}
