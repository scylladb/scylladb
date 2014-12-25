/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 */

#include "database.hh"
#include "net/byteorder.hh"

bool
less_unsigned(const bytes& v1, const bytes& v2) {
    return std::lexicographical_compare(v1.begin(), v1.end(), v2.begin(), v2.end(),
            [](int8_t v1, int8_t v2) { return uint8_t(v1) < uint8_t(v2); });
}

template <typename T>
struct simple_type_impl : data_type::impl {
    simple_type_impl(sstring name) : impl(std::move(name)) {}
    virtual bool less(const bytes& v1, const bytes& v2) override {
        auto& x1 = boost::any_cast<const T&>(deserialize(v1));
        auto& x2 = boost::any_cast<const T&>(deserialize(v2));
        return x1 < x2;
    }
};

struct int_type_impl : simple_type_impl<int32_t> {
    int_type_impl() : simple_type_impl("int") {}
    virtual void serialize(const boost::any& value, std::ostream& out) override {
        auto v = boost::any_cast<const int32_t&>(value);
        auto u = net::hton(uint32_t(v));
        out.write(reinterpret_cast<const char*>(&u), sizeof(u));
    }
    virtual boost::any deserialize(std::istream& in) {
        uint32_t u;
        in.read(reinterpret_cast<char*>(&u), sizeof(u));
        auto v = int32_t(net::ntoh(u));
        return boost::any(v);
    }
};

struct bigint_type_impl : simple_type_impl<int64_t> {
    bigint_type_impl() : simple_type_impl("bigint") {}
    virtual void serialize(const boost::any& value, std::ostream& out) override {
        auto v = boost::any_cast<const int64_t&>(value);
        auto u = net::hton(uint64_t(v));
        out.write(reinterpret_cast<const char*>(&u), sizeof(u));
    }
    virtual boost::any deserialize(std::istream& in) {
        uint64_t u;
        in.read(reinterpret_cast<char*>(&u), sizeof(u));
        auto v = int64_t(net::ntoh(u));
        return boost::any(v);
    }
};

struct string_type_impl : public data_type::impl {
    string_type_impl(sstring name) : impl(name) {}
    virtual void serialize(const boost::any& value, std::ostream& out) override {
        auto& v = boost::any_cast<const sstring&>(value);
        out.write(v.c_str(), v.size());
    }
    virtual boost::any deserialize(std::istream& in) {
        std::vector<char> tmp(std::istreambuf_iterator<char>(in.rdbuf()),
                              std::istreambuf_iterator<char>());
        // FIXME: validation?
        return boost::any(sstring(tmp.data(), tmp.size()));
    }
    virtual bool less(const bytes& v1, const bytes& v2) override {
        return less_unsigned(v1, v2);
    }
};

struct blob_type_impl : public data_type::impl {
    blob_type_impl() : impl("blob") {}
    virtual void serialize(const boost::any& value, std::ostream& out) override {
        auto& v = boost::any_cast<const bytes&>(value);
        out.write(v.c_str(), v.size());
    }
    virtual boost::any deserialize(std::istream& in) {
        std::vector<char> tmp(std::istreambuf_iterator<char>(in.rdbuf()),
                              std::istreambuf_iterator<char>());
        return boost::any(bytes(reinterpret_cast<const char*>(tmp.data()), tmp.size()));
    }
    virtual bool less(const bytes& v1, const bytes& v2) override {
        return less_unsigned(v1, v2);
    }
};

data_type int_type(new int_type_impl);
data_type bigint_type(new bigint_type_impl);
data_type ascii_type(new string_type_impl("ascii"));
data_type blob_type(new blob_type_impl);
data_type varchar_type(new string_type_impl("varchar"));
data_type text_type(new string_type_impl("text"));

partition::partition(column_family& cf)
        : rows(key_compare(cf.clustering_key_type)) {
}

column_family::column_family(data_type partition_key_type,
                             data_type clustering_key_type)
        : partition_key_type(std::move(partition_key_type))
        , clustering_key_type(std::move(clustering_key_type))
        , partitions(key_compare(partition_key_type)) {
}
