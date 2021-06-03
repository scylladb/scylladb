/*
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "test/lib/tmpdir.hh"

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
