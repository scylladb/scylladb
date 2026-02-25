/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include "i_partitioner.hh"
#include "bytes_fwd.hh"

namespace dht {

class murmur3_partitioner final : public i_partitioner {
public:
    murmur3_partitioner() = default;
    virtual const sstring name() const override { return "org.apache.cassandra.dht.Murmur3Partitioner"; }
    virtual token get_token(const schema& s, partition_key_view key) const override;
    virtual token get_token(const sstables::key_view& key) const override;
private:
    token get_token(bytes_view key) const;
    token get_token(uint64_t value) const;
};


}

