/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 */

#ifndef DATABASE_HH_
#define DATABASE_HH_

#include "core/sstring.hh"
#include "core/shared_ptr.hh"
#include "net/byteorder.hh"
#include "utils/UUID.hh"
#include "db_clock.hh"
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
#include <experimental/optional>

// FIXME: should be int8_t
using bytes = basic_sstring<char, uint32_t, 31>;

sstring to_hex(const bytes& b);

using object_opt = std::experimental::optional<boost::any>;

class marshal_exception : public std::exception {
    sstring _why;
public:
    marshal_exception() : _why("marshalling error") {}
    marshal_exception(sstring why) : _why(sstring("marshaling error: ") + why) {}
    virtual const char* why() const { return _why.c_str(); }
};

class abstract_type {
    sstring _name;
public:
    abstract_type(sstring name) : _name(name) {}
    virtual ~abstract_type() {}
    virtual void serialize(const boost::any& value, std::ostream& out) = 0;
    virtual object_opt deserialize(std::istream& in) = 0;
    virtual bool less(const bytes& v1, const bytes& v2) = 0;
    object_opt deserialize(const bytes& v) {
        // FIXME: optimize
        std::istringstream iss(v);
        return deserialize(iss);
    }
    virtual void validate(const bytes& v) {
        // FIXME
    }
    virtual object_opt compose(const bytes& v) {
        return deserialize(v);
    }
    bytes decompose(const boost::any& value) {
        // FIXME: optimize
        std::ostringstream oss;
        serialize(value, oss);
        auto s = oss.str();
        return bytes(s.data(), s.size());
    }
    sstring name() const {
        return _name;
    }
protected:
    template <typename T, typename Compare = std::less<T>>
    bool default_less(const bytes& b1, const bytes& b2, Compare compare = Compare());
};

using data_type = shared_ptr<abstract_type>;

inline
size_t hash_value(const shared_ptr<abstract_type>& x) {
    return std::hash<abstract_type*>()(x.get());
}

template <typename Type>
shared_ptr<abstract_type> data_type_for();

class key_compare {
    shared_ptr<abstract_type> _type;
public:
    key_compare(shared_ptr<abstract_type> type) : _type(type) {}
    bool operator()(const bytes& v1, const bytes& v2) const {
        return _type->less(v1, v2);
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
extern thread_local shared_ptr<abstract_type> int32_type;
extern thread_local shared_ptr<abstract_type> long_type;
extern thread_local shared_ptr<abstract_type> ascii_type;
extern thread_local shared_ptr<abstract_type> bytes_type;
extern thread_local shared_ptr<abstract_type> utf8_type;
extern thread_local shared_ptr<abstract_type> boolean_type;
extern thread_local shared_ptr<abstract_type> timeuuid_type;
extern thread_local shared_ptr<abstract_type> timestamp_type;

template <>
inline
shared_ptr<abstract_type> data_type_for<int32_t>() {
    return int32_type;
}

template <>
inline
shared_ptr<abstract_type> data_type_for<int64_t>() {
    return long_type;
}

template <>
inline
shared_ptr<abstract_type> data_type_for<sstring>() {
    return utf8_type;
}

struct column_definition {
    sstring name;
    shared_ptr<abstract_type> type;
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
    column_family(shared_ptr<abstract_type> partition_key_type, shared_ptr<abstract_type> clustering_key_type);
    // primary key = paritition key + clustering_key
    shared_ptr<abstract_type> partition_key_type;
    shared_ptr<abstract_type> clustering_key_type;
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

class database {
private:
    sstring _datadir;
public:
    explicit database(sstring datadir) : _datadir(datadir) {}
    std::unordered_map<sstring, keyspace> keyspaces;
};

namespace std {

template <>
struct hash<shared_ptr<abstract_type>> : boost::hash<shared_ptr<abstract_type>> {
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

inline
bytes
to_bytes(const utils::UUID& uuid) {
    struct {
        uint64_t msb;
        uint64_t lsb;
    } tmp = { net::hton(uint64_t(uuid.get_most_significant_bits())),
              net::hton(uint64_t(uuid.get_least_significant_bits())) };
    return bytes(reinterpret_cast<char*>(&tmp), 16);
}

// This follows java.util.Comparator
// FIXME: Choose a better place than database.hh
template <typename T>
struct comparator {
    virtual ~comparator() {}
    virtual bool operator()(const T& v1, const T& v2) const = 0;
};

#endif /* DATABASE_HH_ */
