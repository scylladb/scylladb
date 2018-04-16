/*
 * Copyright (C) 2017 ScyllaDB
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
#include <seastar/util/noncopyable_function.hh>
#include "seastarx.hh"

/// \brief Creates an input_stream to read from a supplied buffer
///
/// \param buf Buffer to return from the stream while reading
/// \return resulting input stream
input_stream<char> make_buffer_input_stream(temporary_buffer<char>&& buf);

/// \brief Creates an input_stream to read from a supplied buffer
///
/// \param buf Buffer to return from the stream while reading
/// \param limit_generator Generates limits of chunk sizes
/// \return resulting input stream
input_stream<char> make_buffer_input_stream(temporary_buffer<char>&& buf,
                                            seastar::noncopyable_function<size_t()>&& limit_generator);
