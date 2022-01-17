/*
 *
 *
 */
/*
 * Copyright (C) 2019-present ScyllaDB
 *
 * Modified by ScyllaDB
 */
/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#include "proposal.hh"

namespace service {

namespace paxos {

std::ostream& operator<<(std::ostream& os, const proposal& proposal) {
    return os << "proposal(" << proposal.ballot << ")";
}

} // end of namespace "paxos"
} // endf of namespace "service"
