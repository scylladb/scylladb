/*
 * Copyright (C) 2015-present ScyllaDB
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

#include "i_partitioner.hh"
#include "bytes.hh"
#include <vector>

namespace dht {

class murmur3_partitioner final : public i_partitioner {
public:
    murmur3_partitioner() = default;
    virtual const sstring name() const { return "org.apache.cassandra.dht.Murmur3Partitioner"; }
    virtual token get_token(const schema& s, partition_key_view key) const override;
    virtual token get_token(const sstables::key_view& key) const override;
private:
    token get_token(bytes_view key) const;
    token get_token(uint64_t value) const;
};


}

