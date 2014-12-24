/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 */

#include "database.hh"
#include "net/byteorder.hh"

struct int_type_impl : public data_type::impl {
    int_type_impl() : impl("int") {}
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

struct bigint_type_impl : public data_type::impl {
    bigint_type_impl() : impl("bigint") {}
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
};

data_type int_type(new int_type_impl);
data_type bigint_type(new bigint_type_impl);
data_type ascii_type(new string_type_impl("ascii"));
data_type blob_type(new string_type_impl("blob"));
data_type varchar_type(new string_type_impl("varchar"));
data_type text_type(new string_type_impl("text"));
