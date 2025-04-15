/*
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
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

tmpdir::tmpdir() {
    auto tmp = fs::temp_directory_path();
    for (;;) {
        // Reduce the path length of the created tmp dir. This might seem
        // silly when running with base TMPDIR=/tmp or similar, but
        // in a lot of CI testing, TMPDIR will be a loooooong path into
        // jenkins workdirs or similar -> this path will be 100+ chars long.
        // Again, this should most often not be a problem, _but_ if we
        // for example run something like a sub process of a python server,
        // which will try to create various unix sockets et al for its
        // operations, the TMPDIR base for this must not exceed 107 chars.
        // Note: converting UUID to string first, because for some reason
        // our UUID formatter does not respect width/precision. Feel free to
        // change once it does.
        _path = tmp / fmt::format("scylla-{:.8}", fmt::to_string(utils::make_random_uuid()));
        // Note: this is a slight improvement also, in that we ensure the dir 
        // we use is actually created by us.
        if (fs::create_directories(_path)) {
            break;
        }
    }
}

tmpdir::tmpdir(tmpdir&& other) noexcept : _path(std::exchange(other._path, {})) {}

void tmpdir::operator=(tmpdir&& other) noexcept {
    remove();
    _path = std::exchange(other._path, {});
}

tmpdir::~tmpdir() {
    remove();
}
