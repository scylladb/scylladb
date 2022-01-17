/*
 * Copyright (C) 2017-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
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
