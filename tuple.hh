/*
 * Copyright (C) 2015 Cloudius Systems, Ltd.
 */

#pragma once

#include "types.hh"
#include <iostream>

template<typename T>
static inline
void write(std::ostream& out, T val) {
    auto n_val = net::ntoh(val);
    out.write(reinterpret_cast<char*>(&n_val), sizeof(n_val));
}

// TODO: Add AllowsMissing parameter which will allow to optimize serialized format.
// Currently we default to AllowsMissing = true.
template<bool AllowPrefixes = false>
class tuple_type final : public abstract_type {
private:
    const std::vector<shared_ptr<abstract_type>> types;
    const bool _byte_order_equal;
public:
    using prefix_type = tuple_type<true>;
    using value_type = std::vector<bytes_opt>;

    tuple_type(std::vector<shared_ptr<abstract_type>> types)
        : abstract_type("tuple") // FIXME: append names of member types
        , types(types)
        , _byte_order_equal(std::all_of(types.begin(), types.end(), [] (auto t) {
                return t->is_byte_order_equal();
            }))
    { }

    prefix_type as_prefix() {
        return prefix_type(types);
    }

    /*
     * Format:
     *   <len(value1)><value1><len(value2)><value2>...
     *
     *   if value is missing then len(value) < 0
     */
    void serialize_value(const value_type& values, std::ostream& out) {
        if (AllowPrefixes) {
            assert(values.size() <= types.size());
        } else {
            assert(values.size() == types.size());
        }

        for (auto&& val : values) {
            if (!val) {
                write<uint32_t>(out, uint32_t(-1));
            } else {
                assert(val->size() <= std::numeric_limits<int32_t>::max());
                write<uint32_t>(out, uint32_t(val->size()));
                out.write(val->begin(), val->size());
            }
        }
    }
    bytes serialize_value(const value_type& values) {
        return ::serialize_value(*this, values);
    }
    bytes serialize_value_deep(const std::vector<boost::any>& values) {
        // TODO: Optimize
        std::vector<bytes_opt> partial;
        auto i = types.begin();
        for (auto&& component : values) {
            assert(i != types.end());
            partial.push_back({(*i++)->decompose(component)});
        }
        return serialize_value(partial);
    }
    bytes decompose_value(const value_type& values) {
        return ::serialize_value(*this, values);
    }
    value_type deserialize_value(bytes_view v) {
        std::vector<bytes_opt> result;
        result.reserve(types.size());

        for (auto&& type : types) {
            if (v.empty()) {
                if (AllowPrefixes) {
                    return result;
                } else {
                    throw marshal_exception();
                }
            }
            auto len = read_simple<int32_t>(v);
            if (len < 0) {
                result.emplace_back();
            } else {
                auto positive_len = static_cast<uint32_t>(len);
                if (v.size() < positive_len) {
                    throw marshal_exception();
                }
                result.emplace_back(bytes(v.begin(), v.begin() + positive_len));
                v.remove_prefix(positive_len);
            }
        }

        if (!v.empty()) {
            throw marshal_exception();
        }

        return result;
    }
    object_opt deserialize(bytes_view v) override {
        return {boost::any(deserialize_value(v))};
    }
    void serialize(const boost::any& obj, std::ostream& out) override {
        serialize_value(boost::any_cast<const value_type&>(obj), out);
    }
    virtual bool less(bytes_view b1, bytes_view b2) override {
        return compare(b1, b2) < 0;
    }
    virtual size_t hash(bytes_view v) override {
        if (_byte_order_equal) {
            return std::hash<bytes_view>()(v);
        }
        size_t h = 0;
        auto current_type = types.begin();
        for (auto&& elem : ::deserialize_value(*this, v)) {
            if (elem) {
                h ^= (*current_type)->hash(*elem);
            }
            ++current_type;
        }
        return h;
    }
    virtual int32_t compare(bytes_view b1, bytes_view b2) override {
        if (is_byte_order_comparable()) {
            return compare_unsigned(b1, b2);
        }

        auto v1 = ::deserialize_value(*this, b1);
        auto v2 = ::deserialize_value(*this, b2);

        if (v1.size() != v2.size()) {
            return v1.size() < v2.size() ? -1 : 1;
        }

        if (AllowPrefixes) {
            assert(v1.size() <= types.size());
        } else {
            assert(v1.size() == types.size());
        }

        auto i1 = v1.begin();
        auto i2 = v2.begin();
        auto current_type = types.begin();

        while (i1 != v1.end()) {
            bytes_opt& e1 = *i1++;
            bytes_opt& e2 = *i2++;
            if (bool(e1) != bool(e2)) {
                return e2 ? -1 : 1;
            }
            auto c = (*current_type++)->compare(*e1, *e2);
            if (c != 0) {
                return c;
            }
        }

        return 0;
    }
    virtual bool is_byte_order_equal() const override {
        return _byte_order_equal;
    }
    virtual bool is_byte_order_comparable() const override {
        // We're not byte order comparable because we encode component length as signed integer,
        // which is not byte order comparable.
        // TODO: make the length byte-order comparable by adding numeric_limits<int32_t>::min() when serializing
        return false;
    }
    virtual bytes from_string(sstring_view s) override {
        throw std::runtime_error("not implemented");
    }
    virtual sstring to_string(const bytes& b) override {
        throw std::runtime_error("not implemented");
    }
    /**
     * Returns true iff all components of 'prefix' are equal to corresponding
     * leading components of 'value'.
     *
     * The 'value' is assumed to be serialized using tuple_type<AllowPrefixes=false>
     */
    bool is_prefix_of(bytes_view prefix, bytes_view value) const {
        assert(AllowPrefixes);

        for (auto&& type : _types) {
            if (prefix.empty()) {
                return true;
            }
            assert(!value.empty());
            auto len1 = read_simple<int32_t>(prefix);
            auto len2 = read_simple<int32_t>(value);
            if ((len1 < 0) != (len2 < 0)) {
                // one is empty and another one is not
                return false;
            }
            if (len1 >= 0) {
                // both are not empty
                auto u_len1 = static_cast<uint32_t>(len1);
                auto u_len2 = static_cast<uint32_t>(len2);
                if (prefix.size() < u_len1 || value.size() < u_len2) {
                    throw marshal_exception();
                }
                if (!type->equal(bytes_view(prefix.begin(), u_len1), bytes_view(value.begin(), u_len2))) {
                    return false;
                }
                prefix.remove_prefix(u_len1);
                value.remove_prefix(u_len2);
            }
        }

        if (!prefix.empty() || !value.empty()) {
            throw marshal_exception();
        }

        return true;
    }
    virtual ::shared_ptr<cql3::cql3_type> as_cql3_type() override {
        assert(0);
    }
};

using tuple_prefix = tuple_type<true>;
