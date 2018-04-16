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
