/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "replica/database_fwd.hh"
#include "bytes_ostream.hh"
#include "mutation_fragment.hh"

namespace ser {
template<typename Output>
class writer_of_mutation_partition;
}

class mutation_partition_serializer {
    static size_t size(const schema&, const mutation_partition&);
public:
    using size_type = uint32_t;
private:
    const schema& _schema;
    const mutation_partition& _p;
private:
    template<typename Writer>
    static void write_serialized(Writer&& out, const schema&, const mutation_partition&);
    template<typename Writer>
    static future<> write_serialized_gently(Writer&& out, const schema&, const mutation_partition&);
public:
    using count_type = uint32_t;
    mutation_partition_serializer(const schema&, const mutation_partition&);
public:
    void write(bytes_ostream&) const;
    void write(ser::writer_of_mutation_partition<bytes_ostream>&&) const;
    future<> write_gently(bytes_ostream&) const;
    future<> write_gently(ser::writer_of_mutation_partition<bytes_ostream>&&) const;
};

void serialize_mutation_fragments(const schema& s, tombstone partition_tombstone,
    std::optional<static_row> sr, range_tombstone_list range_tombstones,
    std::deque<clustering_row> clustering_rows, ser::writer_of_mutation_partition<bytes_ostream>&&);
