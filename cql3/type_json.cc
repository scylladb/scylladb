/*
 * Copyright (C) 2019 ScyllaDB
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

#include "cql3/type_json.hh"
#include "concrete_types.hh"
#include "counters.hh"
#include "json.hh"
#include "types/list.hh"
#include "types/map.hh"
#include "types/set.hh"
#include "types/tuple.hh"
#include "types/user.hh"
#include "types/listlike_partial_deserializing_iterator.hh"

static sstring quote_json_string(const sstring& s) {
    return json::value_to_quoted_string(s);
}

static bytes from_json_object_aux(const map_type_impl& t, const Json::Value& value, cql_serialization_format sf) {
    if (!value.isObject()) {
        throw marshal_exception("map_type must be represented as JSON Object");
    }
    std::vector<bytes> raw_map;
    raw_map.reserve(value.size());
    for (auto it = value.begin(); it != value.end(); ++it) {
        if (!t.get_keys_type()->is_compatible_with(*utf8_type)) {
            // Keys in maps can only be strings in JSON, but they can also be a string representation
            // of another JSON type, which needs to be reparsed. Example - map<frozen<list<int>>, int>
            // will be represented like this: { "[1, 3, 6]": 3, "[]": 0, "[1, 2]": 2 }
            Json::Value map_key = json::to_json_value(it.key().asString());
            raw_map.emplace_back(from_json_object(*t.get_keys_type(), map_key, sf));
        } else {
            raw_map.emplace_back(from_json_object(*t.get_keys_type(), it.key(), sf));
        }
        raw_map.emplace_back(from_json_object(*t.get_values_type(), *it, sf));
    }
    return collection_type_impl::pack(raw_map.begin(), raw_map.end(), raw_map.size() / 2, sf);
}

static bytes from_json_object_aux(const set_type_impl& t, const Json::Value& value, cql_serialization_format sf) {
    if (!value.isArray()) {
        throw marshal_exception("set_type must be represented as JSON Array");
    }
    std::vector<bytes> raw_set;
    raw_set.reserve(value.size());
    for (const Json::Value& v : value) {
        raw_set.emplace_back(from_json_object(*t.get_elements_type(), v, sf));
    }
    return collection_type_impl::pack(raw_set.begin(), raw_set.end(), raw_set.size(), sf);
}

static bytes from_json_object_aux(const list_type_impl& t, const Json::Value& value, cql_serialization_format sf) {
    std::vector<bytes> values;
    if (!value.isArray()) {
        throw marshal_exception("list_type must be represented as JSON Array");
    }
    for (const Json::Value& v : value) {
        values.emplace_back(from_json_object(*t.get_elements_type(), v, sf));
    }
    return collection_type_impl::pack(values.begin(), values.end(), values.size(), sf);
}

static bytes from_json_object_aux(const tuple_type_impl& t, const Json::Value& value, cql_serialization_format sf) {
    if (!value.isArray()) {
        throw marshal_exception("tuple_type must be represented as JSON Array");
    }
    if (value.size() > t.all_types().size()) {
        throw marshal_exception(
                format("Too many values ({}) for tuple with size {}", value.size(), t.all_types().size()));
    }
    std::vector<bytes_opt> raw_tuple;
    raw_tuple.reserve(value.size());
    auto ti = t.all_types().begin();
    for (auto vi = value.begin(); vi != value.end(); ++vi, ++ti) {
        raw_tuple.emplace_back(from_json_object(**ti, *vi, sf));
    }
    return t.build_value(std::move(raw_tuple));
}

static bytes from_json_object_aux(const user_type_impl& ut, const Json::Value& value, cql_serialization_format sf) {
    if (!value.isObject()) {
        throw marshal_exception("user_type must be represented as JSON Object");
    }

    std::unordered_set<sstring> remaining_names;
    for (auto vi = value.begin(); vi != value.end(); ++vi) {
        remaining_names.insert(vi.name());
    }

    std::vector<bytes_opt> raw_tuple;
    for (unsigned i = 0; i < ut.field_names().size(); ++i) {
        auto t = ut.all_types()[i];
        auto v = value.get(ut.field_name_as_string(i), Json::Value());
        if (v.isNull()) {
            raw_tuple.push_back(bytes_opt{});
        } else {
            raw_tuple.push_back(from_json_object(*t, v, sf));
        }
        remaining_names.erase(ut.field_name_as_string(i));
    }

    if (!remaining_names.empty()) {
        throw marshal_exception(format(
                "Extraneous field definition for user type {}: {}", ut.get_name_as_string(), *remaining_names.begin()));
    }
    return ut.build_value(std::move(raw_tuple));
}

namespace {
struct from_json_object_visitor {
    const Json::Value& value;
    cql_serialization_format sf;
    bytes operator()(const reversed_type_impl& t) { return from_json_object(*t.underlying_type(), value, sf); }
    template <typename T> bytes operator()(const integer_type_impl<T>& t) {
        if (value.isString()) {
            return t.from_string(value.asString());
        }
        return t.decompose(T(json::to_int64_t(value)));
    }
    bytes operator()(const string_type_impl& t) { return t.from_string(value.asString()); }
    bytes operator()(const bytes_type_impl& t) {
        if (!value.isString()) {
            throw marshal_exception("bytes_type must be represented as string");
        }
        sstring string_value = value.asString();
        if (string_value.size() < 2 && string_value[0] != '0' && string_value[1] != 'x') {
            throw marshal_exception("Blob JSON strings must start with 0x");
        }
        auto v = static_cast<sstring_view>(string_value);
        v.remove_prefix(2);
        return bytes_type->from_string(v);
    }
    bytes operator()(const boolean_type_impl& t) {
        if (!value.isBool()) {
            throw marshal_exception(format("Invalid JSON object {}", value.toStyledString()));
        }
        return t.decompose(value.asBool());
    }
    bytes operator()(const timestamp_date_base_class& t) {
        if (!value.isString() && !value.isIntegral()) {
            throw marshal_exception("timestamp_type must be represented as string or integer");
        }
        if (value.isIntegral()) {
            return long_type->decompose(json::to_int64_t(value));
        }
        return t.from_string(value.asString());
    }
    bytes operator()(const timeuuid_type_impl& t) {
        if (!value.isString()) {
            throw marshal_exception(format("{} must be represented as string in JSON", value.toStyledString()));
        }
        return t.from_string(value.asString());
    }
    bytes operator()(const simple_date_type_impl& t) { return t.from_string(value.asString()); }
    bytes operator()(const time_type_impl& t) { return t.from_string(value.asString()); }
    bytes operator()(const uuid_type_impl& t) { return t.from_string(value.asString()); }
    bytes operator()(const inet_addr_type_impl& t) { return t.from_string(value.asString()); }
    template <typename T> bytes operator()(const floating_type_impl<T>& t) {
        if (value.isString()) {
            return t.from_string(value.asString());
        }
        if (!value.isDouble()) {
            throw marshal_exception("JSON value must be represented as double or string");
        }
        if constexpr (std::is_same<T, float>::value) {
            return t.decompose(value.asFloat());
        } else if constexpr (std::is_same<T, double>::value) {
            return t.decompose(value.asDouble());
        } else {
            throw marshal_exception("Only float/double types can be parsed from JSON floating point object");
        }
    }
    bytes operator()(const varint_type_impl& t) {
        if (value.isString()) {
            return t.from_string(value.asString());
        }
        return t.from_string(json::to_sstring(value));
    }
    bytes operator()(const decimal_type_impl& t) {
        if (value.isString()) {
            return t.from_string(value.asString());
        } else if (!value.isNumeric()) {
            throw marshal_exception(
                    format("{} must be represented as numeric or string in JSON", value.toStyledString()));
        }

        return t.from_string(json::to_sstring(value));
    }
    bytes operator()(const counter_type_impl& t) {
        if (!value.isIntegral()) {
            throw marshal_exception("Counters must be represented as JSON integer");
        }
        return counter_cell_view::total_value_type()->decompose(json::to_int64_t(value));
    }
    bytes operator()(const duration_type_impl& t) {
        if (!value.isString()) {
            throw marshal_exception(format("{} must be represented as string in JSON", value.toStyledString()));
        }
        return t.from_string(value.asString());
    }
    bytes operator()(const empty_type_impl& t) { return bytes(); }
    bytes operator()(const map_type_impl& t) { return from_json_object_aux(t, value, sf); }
    bytes operator()(const set_type_impl& t) { return from_json_object_aux(t, value, sf); }
    bytes operator()(const list_type_impl& t) { return from_json_object_aux(t, value, sf); }
    bytes operator()(const tuple_type_impl& t) { return from_json_object_aux(t, value, sf); }
    bytes operator()(const user_type_impl& t) { return from_json_object_aux(t, value, sf); }
};
}

bytes from_json_object(const abstract_type& t, const Json::Value& value, cql_serialization_format sf) {
    return visit(t, from_json_object_visitor{value, sf});
}

template <typename T> static T compose_value(const integer_type_impl<T>& t, bytes_view bv) {
    if (bv.size() != sizeof(T)) {
        throw marshal_exception(format("Size mismatch for type {}: got {:d} bytes", t.name(), bv.size()));
    }
    return read_be<T>(reinterpret_cast<const char*>(bv.data()));
}

static sstring to_json_string_aux(const map_type_impl& t, bytes_view bv) {
    std::ostringstream out;
    auto sf = cql_serialization_format::internal();

    out << '{';
    auto size = read_collection_size(bv, sf);
    for (int i = 0; i < size; ++i) {
        auto kb = read_collection_value(bv, sf);
        auto vb = read_collection_value(bv, sf);

        if (i > 0) {
            out << ", ";
        }

        // Valid keys in JSON map must be quoted strings
        sstring string_key = to_json_string(*t.get_keys_type(), kb);
        bool is_unquoted = string_key.empty() || string_key[0] != '"';
        if (is_unquoted) {
            out << '"';
        }
        out << string_key;
        if (is_unquoted) {
            out << '"';
        }
        out << ": ";
        out << to_json_string(*t.get_values_type(), vb);
    }
    out << '}';
    return out.str();
}

static sstring to_json_string_aux(const set_type_impl& t, bytes_view bv) {
    using llpdi = listlike_partial_deserializing_iterator;
    std::ostringstream out;
    bool first = true;
    auto sf = cql_serialization_format::internal();
    out << '[';
    std::for_each(llpdi::begin(bv, sf), llpdi::end(bv, sf), [&first, &out, &t] (bytes_view e) {
        if (first) {
            first = false;
        } else {
            out << ", ";
        }
        out << to_json_string(*t.get_elements_type(), e);
    });
    out << ']';
    return out.str();
}

static sstring to_json_string_aux(const list_type_impl& t, bytes_view bv) {
    using llpdi = listlike_partial_deserializing_iterator;
    std::ostringstream out;
    bool first = true;
    auto sf = cql_serialization_format::internal();
    out << '[';
    std::for_each(llpdi::begin(bv, sf), llpdi::end(bv, sf), [&first, &out, &t] (bytes_view e) {
        if (first) {
            first = false;
        } else {
            out << ", ";
        }
        out << to_json_string(*t.get_elements_type(), e);
    });
    out << ']';
    return out.str();
}

static sstring to_json_string_aux(const tuple_type_impl& t, bytes_view bv) {
    std::ostringstream out;
    out << '[';

    auto ti = t.all_types().begin();
    auto vi = tuple_deserializing_iterator::start(bv);
    while (ti != t.all_types().end() && vi != tuple_deserializing_iterator::finish(bv)) {
        if (ti != t.all_types().begin()) {
            out << ", ";
        }
        if (*vi) {
            // TODO(sarna): We can avoid copying if to_json_string accepted bytes_view
            out << to_json_string(**ti, **vi);
        } else {
            out << "null";
        }
        ++ti;
        ++vi;
    }

    out << ']';
    return out.str();
}

static sstring to_json_string_aux(const user_type_impl& t, bytes_view bv) {
    std::ostringstream out;
    out << '{';

    auto ti = t.all_types().begin();
    auto vi = tuple_deserializing_iterator::start(bv);
    int i = 0;
    while (ti != t.all_types().end() && vi != tuple_deserializing_iterator::finish(bv)) {
        if (ti != t.all_types().begin()) {
            out << ", ";
        }
        out << quote_json_string(t.field_name_as_string(i)) << ": ";
        if (*vi) {
            //TODO(sarna): We can avoid copying if to_json_string accepted bytes_view
            out << to_json_string(**ti, **vi);
        } else {
            out << "null";
        }
        ++ti;
        ++i;
        ++vi;
    }

    out << '}';
    return out.str();
}

namespace {
struct to_json_string_visitor {
    bytes_view bv;
    sstring operator()(const reversed_type_impl& t) { return to_json_string(*t.underlying_type(), bv); }
    template <typename T> sstring operator()(const integer_type_impl<T>& t) { return to_sstring(compose_value(t, bv)); }
    template <typename T> sstring operator()(const floating_type_impl<T>& t) {
        if (bv.empty()) {
            throw exceptions::invalid_request_exception("Cannot create JSON string - deserialization error");
        }
        auto v = t.deserialize(bv);
        T d = value_cast<T>(v);
        if (std::isnan(d) || std::isinf(d)) {
            return "null";
        }
        return to_sstring(d);
    }
    sstring operator()(const uuid_type_impl& t) { return quote_json_string(t.to_string(bv)); }
    sstring operator()(const inet_addr_type_impl& t) { return quote_json_string(t.to_string(bv)); }
    sstring operator()(const string_type_impl& t) { return quote_json_string(t.to_string(bv)); }
    sstring operator()(const bytes_type_impl& t) { return quote_json_string("0x" + t.to_string(bv)); }
    sstring operator()(const boolean_type_impl& t) { return t.to_string(bv); }
    sstring operator()(const timestamp_date_base_class& t) { return quote_json_string(t.to_string(bv)); }
    sstring operator()(const timeuuid_type_impl& t) { return quote_json_string(t.to_string(bv)); }
    sstring operator()(const map_type_impl& t) { return to_json_string_aux(t, bv); }
    sstring operator()(const set_type_impl& t) { return to_json_string_aux(t, bv); }
    sstring operator()(const list_type_impl& t) { return to_json_string_aux(t, bv); }
    sstring operator()(const tuple_type_impl& t) { return to_json_string_aux(t, bv); }
    sstring operator()(const user_type_impl& t) { return to_json_string_aux(t, bv); }
    sstring operator()(const simple_date_type_impl& t) { return quote_json_string(t.to_string(bv)); }
    sstring operator()(const time_type_impl& t) { return t.to_string(bv); }
    sstring operator()(const empty_type_impl& t) { return "null"; }
    sstring operator()(const duration_type_impl& t) {
        auto v = t.deserialize(bv);
        if (v.is_null()) {
            throw exceptions::invalid_request_exception("Cannot create JSON string - deserialization error");
        }
        return quote_json_string(t.to_string(bv));
    }
    sstring operator()(const counter_type_impl& t) {
        // It will be called only from cql3 layer while processing query results.
        return to_json_string(*counter_cell_view::total_value_type(), bv);
    }
    sstring operator()(const decimal_type_impl& t) {
        if (bv.empty()) {
            throw exceptions::invalid_request_exception("Cannot create JSON string - deserialization error");
        }
        auto v = t.deserialize(bv);
        return value_cast<big_decimal>(v).to_string();
    }
    sstring operator()(const varint_type_impl& t) {
        if (bv.empty()) {
            throw exceptions::invalid_request_exception("Cannot create JSON string - deserialization error");
        }
        auto v = t.deserialize(bv);
        return value_cast<utils::multiprecision_int>(v).str();
    }
};
}

sstring to_json_string(const abstract_type& t, bytes_view bv) {
    return visit(t, to_json_string_visitor{bv});
}
