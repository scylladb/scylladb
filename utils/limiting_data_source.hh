/*
 * Copyright (C) 2017-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <stddef.h>
#include <seastar/util/noncopyable_function.hh>

namespace seastar {

class data_source;

}

/// \brief Creates an data_source from another data_source but returns its data in chunks not bigger than a given limit
///
/// \param src Source data_source from which data will be taken
/// \return resulting data_source that returns data in chunks not bigger than a given limit
seastar::data_source make_limiting_data_source(seastar::data_source&& src,
                                               seastar::noncopyable_function<size_t()>&& limit_generator);
