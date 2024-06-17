/*
 * Copyright 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */
#include "gms/inet_address.hh"
#include "gms/gossip_digest_syn.hh"

namespace gms {
verb [[with_client_info, with_timeout]] gossip_echo (int64_t generation_number [[version 4.6.0]], bool notify_up [[version 6.1.0]])
verb [[one_way]] gossip_shutdown (gms::inet_address from, int64_t generation_number [[version 4.6.0]])
verb [[with_client_info, one_way]] gossip_digest_syn (gms::gossip_digest_syn syn)
verb [[with_client_info, one_way]] gossip_digest_ack (gms::gossip_digest_ack ask)
verb [[with_client_info, one_way]] gossip_digest_ack2 (gms::gossip_digest_ack2 ask)
verb [[with_client_info, with_timeout]] gossip_get_endpoint_states (gms::gossip_get_endpoint_states_request req) -> gms::gossip_get_endpoint_states_response
}
