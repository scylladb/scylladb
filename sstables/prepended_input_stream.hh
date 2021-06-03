/*
 * Copyright (C) 2017-present ScyllaDB
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

#pragma once

#include <seastar/core/iostream.hh>
#include <seastar/core/temporary_buffer.hh>
#include "seastarx.hh"

/// \brief Creates an input_stream to read from a supplied buffer first
/// and then use a supplied data source
///
/// \param buf Buffer with data to be read first
/// \param ds Data source that is used for reading data once the prepending buffer is consumed in full
/// \return resulting input stream
input_stream<char> make_prepended_input_stream(temporary_buffer<char>&& buf, data_source&& ds);
