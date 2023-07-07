/*
 * Copyright 2012-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "inet_address_vectors.hh"
#include "message/messaging_service.hh"

#include "gms/inet_address_serializer.hh"

#include "idl/frozen_mutation.idl.hh"
#include "idl/tracing.idl.hh"
#include "idl/view.idl.hh"
#include "idl/consistency_level.idl.hh"
#include "idl/read_command.idl.hh"
#include "idl/query.idl.hh"
#include "idl/result.idl.hh"
#include "idl/per_partition_rate_limit_info.idl.hh"
#include "idl/keys.idl.hh"
#include "idl/uuid.idl.hh"
#include "idl/storage_service.idl.hh"

verb [[with_client_info, with_timeout, one_way]] mutation (frozen_mutation fm [[ref]], inet_address_vector_replica_set forward [[ref]], gms::inet_address reply_to, unsigned shard, uint64_t response_id, std::optional<tracing::trace_info> trace_info [[ref]] [[version 1.3.0]], db::per_partition_rate_limit::info rate_limit_info [[version 5.1.0]], service::fencing_token fence [[version 5.4.0]]);
verb [[with_client_info, one_way]] mutation_done (unsigned shard, uint64_t response_id, db::view::update_backlog backlog [[version 3.1.0]]);
verb [[with_client_info, one_way]] mutation_failed (unsigned shard, uint64_t response_id, size_t num_failed, db::view::update_backlog backlog [[version 3.1.0]], replica::exception_variant exception [[version 5.1.0]]);
verb [[with_client_info, with_timeout]] counter_mutation (std::vector<frozen_mutation> fms, db::consistency_level cl, std::optional<tracing::trace_info> trace_info [[ref]], service::fencing_token fence [[version 5.4.0]]) -> replica::exception_variant [[version 5.4.0]];
verb [[with_client_info, with_timeout, one_way]] hint_mutation (frozen_mutation fm [[ref]], inet_address_vector_replica_set forward [[ref]], gms::inet_address reply_to, unsigned shard, uint64_t response_id, std::optional<tracing::trace_info> trace_info [[ref]] [[version 1.3.0]] /* this verb was mistakenly introduced with optional trace_info */, service::fencing_token fence [[version 5.4.0]]);
verb [[with_client_info, with_timeout]] read_data (query::read_command cmd [[ref]], ::compat::wrapping_partition_range pr, query::digest_algorithm digest [[version 3.0.0]], db::per_partition_rate_limit::info rate_limit_info [[version 5.1.0]], service::fencing_token fence [[version 5.4.0]]) -> query::result [[lw_shared_ptr]], cache_temperature [[version 2.0.0]], replica::exception_variant [[version 5.1.0]];
verb [[with_client_info, with_timeout]] read_mutation_data (query::read_command cmd [[ref]], ::compat::wrapping_partition_range pr, service::fencing_token fence [[version 5.4.0]]) -> reconcilable_result [[lw_shared_ptr]], cache_temperature [[version 2.0.0]], replica::exception_variant [[version 5.1.0]];
verb [[with_client_info, with_timeout]] read_digest (query::read_command cmd [[ref]], ::compat::wrapping_partition_range pr, query::digest_algorithm digest [[version 3.0.0]], db::per_partition_rate_limit::info rate_limit_info [[version 5.1.0]], service::fencing_token fence [[version 5.4.0]]) -> query::result_digest, api::timestamp_type [[version 1.2.0]], cache_temperature [[version 2.0.0]], replica::exception_variant [[version 5.1.0]], std::optional<full_position> [[version 5.2.0]];
verb [[with_timeout]] truncate (sstring, sstring);
verb [[with_client_info, with_timeout]] paxos_prepare (query::read_command cmd [[ref]], partition_key key [[ref]], utils::UUID ballot, bool only_digest, query::digest_algorithm da, std::optional<tracing::trace_info> trace_info [[ref]]) -> service::paxos::prepare_response [[unique_ptr]];
verb [[with_client_info, with_timeout]] paxos_accept (service::paxos::proposal proposal [[ref]], std::optional<tracing::trace_info> trace_info [[ref]]) -> bool;
verb [[with_client_info, with_timeout, one_way]] paxos_learn (service::paxos::proposal decision [[ref]], inet_address_vector_replica_set forward [[ref]], gms::inet_address reply_to, unsigned shard, uint64_t response_id, std::optional<tracing::trace_info> trace_info [[ref]]);
verb [[with_client_info, with_timeout, one_way]] paxos_prune (table_schema_version schema_id, partition_key key [[ref]], utils::UUID ballot, std::optional<tracing::trace_info> trace_info [[ref]]);
