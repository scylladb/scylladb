/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "dht/logstor_partitioner.hh"

#include "replica/logstor/key_utils.hh"
#include "utils/class_registrator.hh"

namespace dht {

token logstor_partitioner::get_token(const schema& s, partition_key_view key) const {
    return replica::logstor::token_from_key_hash(replica::logstor::compute_key_hash(s, key));
}

token logstor_partitioner::get_token(const sstables::key_view& key) const {
    return replica::logstor::token_from_key_hash(replica::logstor::compute_key_hash(key));
}

using registry = class_registrator<i_partitioner, logstor_partitioner>;
static registry registrator(logstor_partitioner::classname);
static registry registrator_short_name("LogstorHashPrefixPartitioner");

}
