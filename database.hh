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

// FIXME: should be int8_t
using bytes = basic_sstring<char, uint32_t, 31>;

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
        virtual bool less(const bytes& v1, const bytes& v2) = 0;
        boost::any deserialize(const bytes& v) {
            // FIXME: optimize
            std::istringstream iss(v);
            return deserialize(iss);
        }
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
    bytes decompose(const boost::any& value) {
        // FIXME: optimize
        std::ostringstream oss;
        _impl->serialize(value, oss);
        auto s = oss.str();
        return bytes(s.data(), s.size());
    }
    boost::any deserialize(std::istream& in) {
        return _impl->deserialize(in);
    }
    boost::any deserialize(const bytes& v) {
        return _impl->deserialize(v);
    }
    bool operator==(const data_type& x) const {
        return _impl == x._impl;
    }
    bool operator!=(const data_type& x) const {
        return _impl != x._impl;
    }
    bool less(const bytes& v1, const bytes& v2) const {
        return _impl->less(v1, v2);
    }
    friend size_t hash_value(const data_type& x) {
        return std::hash<impl*>()(x._impl);
    }
};

class key_compare {
    data_type _type;
public:
    key_compare(data_type type) : _type(type) {}
    bool operator()(const bytes& v1, const bytes& v2) const {
        return _type.less(v1, v2);
    }
};

struct row;
struct paritition;
struct column_family;

struct row {
    std::vector<bytes> cells;
};

struct partition {
    explicit partition(column_family& cf);
    row static_columns;
    // row key within partition -> row
    std::map<bytes, row, key_compare> rows;
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
    struct name_compare {
        bool operator()(const column_definition& cd1, const column_definition& cd2) const {
            return std::lexicographical_compare(
                    cd1.name.begin(), cd1.name.end(),
                    cd2.name.begin(), cd2.name.end(),
                    [] (char c1, char c2) { return uint8_t(c1) < uint8_t(c1); });
        }
    };
};

struct column_family {
    column_family(data_type partition_key_type, data_type clustering_key_type);
    // primary key = paritition key + clustering_key
    data_type partition_key_type;
    data_type clustering_key_type;
    std::vector<column_definition> partition_key;
    std::vector<column_definition> clustering_key;
    std::vector<column_definition> column_defs; // sorted by name
    partition& find_or_create_partition(const bytes& key);
    row& find_or_create_row(const bytes& partition_key, const bytes& clustering_key);
    partition* find_partition(const bytes& key);
    row* find_row(const bytes& partition_key, const bytes& clustering_key);
    // partition key -> partition
    std::map<bytes, partition, key_compare> partitions;
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

inline
bytes
to_bytes(const char* x) {
    return bytes(reinterpret_cast<const char*>(x), std::strlen(x));
}

inline
bytes
to_bytes(const std::string& x) {
    return bytes(reinterpret_cast<const char*>(x.data()), x.size());
}

inline
bytes
to_bytes(const sstring& x) {
    return bytes(reinterpret_cast<const char*>(x.c_str()), x.size());
}

#endif /* DATABASE_HH_ */
