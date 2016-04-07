/*
 * Copyright (C) 2015 ScyllaDB
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

#include "version.hh"

#include <seastar/core/print.hh>

static const char scylla_version_str[] = SCYLLA_VERSION;
static const char scylla_release_str[] = SCYLLA_RELEASE;

std::string scylla_version()
{
    return sprint("%s-%s", scylla_version_str, scylla_release_str);
}

// get the version number into writeable memory, so we can grep for it if we get a core dump
std::string version_stamp_for_core
    = "VERSION VERSION VERSION $Id: " + scylla_version() + " $ VERSION VERSION VERSION";
