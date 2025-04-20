/*
 * Copyright 2019-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "idl/result.idl.hh"

namespace service {
namespace paxos {

class proposal {
    utils::UUID ballot;
    frozen_mutation update;
};

class promise {
    std::optional<service::paxos::proposal> accepted_proposal;
    std::optional<service::paxos::proposal> most_recent_commit;
    std::optional<std::variant<query::result, query::result_digest>> get_data_or_digest();
};

}
}
