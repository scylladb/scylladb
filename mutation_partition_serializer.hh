/*
 * Copyright (C) 2015 Cloudius Systems, Ltd.
 */

#pragma once

#include "utils/data_input.hh"
#include "utils/data_output.hh"
#include "database_fwd.hh"
#include "mutation_partition_view.hh"
#include "bytes_ostream.hh"

class mutation_partition_serializer {
    static size_t size(const schema&, const mutation_partition&);
public:
    using size_type = uint32_t;
private:
    const schema& _schema;
    const mutation_partition& _p;
    size_type _size;
public:
    using count_type = uint32_t;
    mutation_partition_serializer(const schema&, const mutation_partition&);
public:
    size_t size() const { return _size + sizeof(size_type); }
    size_t size_without_framing() const { return _size ; }
    void write(data_output&) const;
    void write_without_framing(data_output&) const;
    void write(bytes_ostream&) const;
public:
    static mutation_partition_view read_as_view(data_input&);
    static mutation_partition read(data_input&, schema_ptr);
};
