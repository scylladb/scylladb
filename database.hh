/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 */

#ifndef DATABASE_HH_
#define DATABASE_HH_

#include "core/sstring.hh"
#include <functional>
#include <boost/any.hpp>
#include <cstdint>
#include <boost/variant.hpp>
#include <unordered_map>
#include <map>
#include <set>
#include <vector>
#include <iostream>
#include <boost/functional/hash.hpp>

using bytes = basic_sstring<uint8_t, uint32_t, 31>;

struct row {
    std::vector<boost::any> cells;
};

using key_compare = std::function<bool (const boost::any&, const boost::any&)>;

struct partition {
    row static_columns;
    // row key within partition -> row
    std::map<boost::any, row, key_compare> rows;
};

class data_type {
public:
    // Hide the virtual stuff behind an impl class.  This allows us to treat
    // data_type as a normal value - we can copy, assign, and destroy it
    // without worrying about the destructor.
    struct impl {
        sstring name;
        impl(sstring name) : name(name) {}
        virtual ~impl() {}
        virtual void serialize(const boost::any& value, std::ostream& out) = 0;
        virtual boost::any deserialize(std::istream& in) = 0;
    };
private:
    impl* _impl;
public:
    explicit data_type(impl* impl) : _impl(impl) {}
    static data_type find(const sstring& name);
    const sstring& name() const { return _impl->name; }
    void serialize(const boost::any& value, std::ostream& out) {
        return _impl->serialize(value, out);
    }
    boost::any deserialize(std::istream& in) {
        return _impl->deserialize(in);
    }
    bool operator==(const data_type& x) const {
        return _impl == x._impl;
    }
    bool operator!=(const data_type& x) const {
        return _impl != x._impl;
    }
    friend size_t hash_value(const data_type& x) {
        return std::hash<impl*>()(x._impl);
    }
};

// FIXME: add missing types
extern data_type int_type;
extern data_type bigint_type;
extern data_type ascii_type;
extern data_type blob_type;
extern data_type varchar_type;
extern data_type text_type;

struct column_definition {
    sstring name;
    data_type type;
};

struct column_family {
    // primary key = paritition key + clustering_key
    std::vector<column_definition> partition_key;
    std::vector<column_definition> clustering_key;
    std::vector<column_definition> column_defs;
    std::unordered_map<sstring, unsigned> column_names;
    // partition key -> partition
    std::map<boost::any, partition, key_compare> partitions;
};

struct keyspace {
    std::unordered_map<sstring, column_family> column_families;
};

struct database {
    std::unordered_map<sstring, keyspace> keyspaces;
};

namespace std {

template <>
struct hash<data_type> : boost::hash<data_type> {
};

}

#endif /* DATABASE_HH_ */
