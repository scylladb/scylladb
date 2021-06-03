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

#include "streaming/stream_reason.hh"
#include <ostream>

namespace streaming {

std::ostream& operator<<(std::ostream& out, stream_reason r) {
    switch (r) {
	case stream_reason::unspecified: out << "unspecified"; break;
	case stream_reason::bootstrap: out << "bootstrap"; break;
	case stream_reason::decommission: out << "decommission"; break;
	case stream_reason::removenode: out << "removenode"; break;
	case stream_reason::rebuild: out << "rebuild"; break;
	case stream_reason::repair: out << "repair"; break;
	case stream_reason::replace: out << "replace"; break;
    }
    return out;
}

}
