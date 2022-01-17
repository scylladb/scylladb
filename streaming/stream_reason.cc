/*
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
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
