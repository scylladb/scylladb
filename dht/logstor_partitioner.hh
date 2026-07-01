/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include "dht/i_partitioner.hh"

namespace dht {

class logstor_partitioner final : public i_partitioner {
public:
    static inline const sstring classname = "com.scylladb.dht.LogstorHashPrefixPartitioner";

    logstor_partitioner() = default;

    const sstring name() const override {
        return classname;
    }

    token get_token(const schema& s, partition_key_view key) const override;
    token get_token(const sstables::key_view& key) const override;
};

}
