/*
 * Copyright (C) 2019-present ScyllaDB
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
#include "utils/rjson.hh"
#include "types/list.hh"
#include "types/map.hh"
#include "types/set.hh"
#include "types/tuple.hh"
#include "types/user.hh"
#include "types/listlike_partial_deserializing_iterator.hh"
#include "utils/managed_bytes.hh"
#include "exceptions/exceptions.hh"

static inline bool is_control_char(char c) {
    return c >= 0 && c <= 0x1F;
}

static inline bool needs_escaping(const sstring& s) {
    return std::any_of(s.begin(), s.end(), [](char c) {return is_control_char(c) || c == '"' || c == '\\';});
}


static sstring quote_json_string(const sstring& value) {
    if (!needs_escaping(value)) {
        return format("\"{}\"", value);
    }
    std::ostringstream oss;
    oss << std::hex << std::uppercase << std::setfill('0');
    oss.put('"');
    for (char c : value) {
        switch (c) {
        case '"':
            oss.put('\\').put('"');
            break;
        case '\\':
            oss.put('\\').put('\\');
            break;
        case '\b':
            oss.put('\\').put('b');
            break;
        case '\f':
            oss.put('\\').put('f');
            break;
        case '\n':
            oss.put('\\').put('n');
            break;
        case '\r':
            oss.put('\\').put('r');
            break;
        case '\t':
            oss.put('\\').put('t');
            break;
        default:
            if (is_control_char(c)) {
                oss.put('\\').put('u') << std::setw(4) << static_cast<int>(c);
            } else {
                oss.put(c);
            }
            break;
        }
    }
    oss.put('"');
    return oss.str();
}

static int64_t to_int64_t(const rjson::value& value) {
    if (value.IsInt64()) {
        return value.GetInt64();
    } else if (value.IsInt()) {
        return value.GetInt();
    } else if (value.IsUint()) {
        return value.GetUint();
    } else if (value.GetUint64()) {
        return value.GetUint64(); //NOTICE: large uint64_t values will get overflown
    }
    throw marshal_exception(format("Incorrect JSON value for int64 type: {}", value));
}

static bytes from_json_object_aux(const map_type_impl& t, const rjson::value& value, cql_serialization_format sf) {
    if (!value.IsObject()) {
        throw marshal_exception("map_type must be represented as JSON Object");
    }
    std::vector<bytes> raw_map;
    raw_map.reserve(value.MemberCount());
    for (auto it = value.MemberBegin(); it != value.MemberEnd(); ++it) {
        if (!t.get_keys_type()->is_compatible_with(*utf8_type)) {
            // Keys in maps can only be strings in JSON, but they can also be a string representation
            // of another JSON type, which needs to be reparsed. Example - map<frozen<list<int>>, int>
            // will be represented like this: { "[1, 3, 6]": 3, "[]": 0, "[1, 2]": 2 }
            rjson::value map_key = rjson::parse(rjson::to_string_view(it->name));
            raw_map.emplace_back(from_json_object(*t.get_keys_type(), map_key, sf));
        } else {
            raw_map.emplace_back(from_json_object(*t.get_keys_type(), it->name, sf));
        }
        raw_map.emplace_back(from_json_object(*t.get_values_type(), it->value, sf));
    }
    return collection_type_impl::pack(raw_map.begin(), raw_map.end(), raw_map.size() / 2, sf);
}

static bytes from_json_object_aux(const set_type_impl& t, const rjson::value& value, cql_serialization_format sf) {
    if (!value.IsArray()) {
        throw marshal_exception("set_type must be represented as JSON Array");
    }
    std::vector<bytes> raw_set;
    raw_set.reserve(value.Size());
    for (const rjson::value& v : value.GetArray()) {
        raw_set.emplace_back(from_json_object(*t.get_elements_type(), v, sf));
    }
    return collection_type_impl::pack(raw_set.begin(), raw_set.end(), raw_set.size(), sf);
}

static bytes from_json_object_aux(const list_type_impl& t, const rjson::value& value, cql_serialization_format sf) {
    std::vector<bytes> values;
    if (!value.IsArray()) {
        throw marshal_exception("list_type must be represented as JSON Array");
    }
    for (const rjson::value& v : value.GetArray()) {
        values.emplace_back(from_json_object(*t.get_elements_type(), v, sf));
    }
    return collection_type_impl::pack(values.begin(), values.end(), values.size(), sf);
}

static bytes from_json_object_aux(const tuple_type_impl& t, const rjson::value& value, cql_serialization_format sf) {
    if (!value.IsArray()) {
        throw marshal_exception("tuple_type must be represented as JSON Array");
    }
    if (value.Size() > t.all_types().size()) {
        throw marshal_exception(
                format("Too many values ({}) for tuple with size {}", value.Size(), t.all_types().size()));
    }
    std::vector<bytes_opt> raw_tuple;
    raw_tuple.reserve(value.Size());
    auto ti = t.all_types().begin();
    for (auto vi = value.Begin(); vi != value.End(); ++vi, ++ti) {
        raw_tuple.emplace_back(from_json_object(**ti, *vi, sf));
    }
    return t.build_value(std::move(raw_tuple));
}

static bytes from_json_object_aux(const user_type_impl& ut, const rjson::value& value, cql_serialization_format sf) {
    if (!value.IsObject()) {
        throw marshal_exception("user_type must be represented as JSON Object");
    }

    std::unordered_set<std::string_view> remaining_names;
    for (auto vi = value.MemberBegin(); vi != value.MemberEnd(); ++vi) {
        remaining_names.insert(rjson::to_string_view(vi->name));
    }

    std::vector<bytes_opt> raw_tuple;
    for (unsigned i = 0; i < ut.field_names().size(); ++i) {
        auto t = ut.all_types()[i];
        const rjson::value* v = rjson::find(value, std::string_view(ut.field_name_as_string(i)));
        if (!v || v->IsNull()) {
            raw_tuple.push_back(bytes_opt{});
        } else {
            raw_tuple.push_back(from_json_object(*t, *v, sf));
        }
        remaining_names.erase(std::string_view(ut.field_name_as_string(i)));
    }

    if (!remaining_names.empty()) {
        throw marshal_exception(format(
                "Extraneous field definition for user type {}: {}", ut.get_name_as_string(), *remaining_names.begin()));
    }
    return ut.build_value(std::move(raw_tuple));
}

namespace {
struct from_json_object_visitor {
    const rjson::value& value;
    cql_serialization_format sf;
    bytes operator()(const reversed_type_impl& t) { return from_json_object(*t.underlying_type(), value, sf); }
    template <typename T> bytes operator()(const integer_type_impl<T>& t) {
        if (value.IsString()) {
            return t.from_string(rjson::to_string_view(value));
        }
        return t.decompose(T(to_int64_t(value)));
    }
    bytes operator()(const string_type_impl& t) { return t.from_string(rjson::to_string_view(value)); }
    bytes operator()(const bytes_type_impl& t) {
        if (!value.IsString()) {
            throw marshal_exception("bytes_type must be represented as string");
        }
        std::string_view string_v = rjson::to_string_view(value);
        if (string_v.size() < 2 && string_v[0] != '0' && string_v[1] != 'x') {
            throw marshal_exception("Blob JSON strings must start with 0x");
        }
        string_v.remove_prefix(2);
        return bytes_type->from_string(string_v);
    }
    bytes operator()(const boolean_type_impl& t) {
        if (!value.IsBool()) {
            throw marshal_exception(format("Invalid JSON object {}", value));
        }
        return t.decompose(value.GetBool());
    }
    bytes operator()(const timestamp_date_base_class& t) {
        if (!value.IsString() && !value.IsNumber()) {
            throw marshal_exception("timestamp_type must be represented as string or integer");
        }
        if (value.IsNumber()) {
            return long_type->decompose(to_int64_t(value));
        }
        return t.from_string(rjson::to_string_view(value));
    }
    bytes operator()(const timeuuid_type_impl& t) {
        if (!value.IsString()) {
            throw marshal_exception(format("{} must be represented as string in JSON", value));
        }
        return t.from_string(rjson::to_string_view(value));
    }
    bytes operator()(const simple_date_type_impl& t) { return t.from_string(rjson::to_string_view(value)); }
    bytes operator()(const time_type_impl& t) { return t.from_string(rjson::to_string_view(value)); }
    bytes operator()(const uuid_type_impl& t) { return t.from_string(rjson::to_string_view(value)); }
    bytes operator()(const inet_addr_type_impl& t) { return t.from_string(rjson::to_string_view(value)); }
    template <typename T> bytes operator()(const floating_type_impl<T>& t) {
        if (value.IsString()) {
            return t.from_string(rjson::to_string_view(value));
        }
        if (!value.IsNumber()) {
            throw marshal_exception("JSON value must be represented as double or string");
        }
        if constexpr (std::is_same<T, float>::value) {
            return t.decompose(value.GetFloat());
        } else if constexpr (std::is_same<T, double>::value) {
            return t.decompose(value.GetDouble());
        } else {
            throw marshal_exception("Only float/double types can be parsed from JSON floating point object");
        }
    }
    bytes operator()(const varint_type_impl& t) {
        if (value.IsString()) {
            return t.from_string(rjson::to_string_view(value));
        }
        return t.from_string(rjson::print(value));
    }
    bytes operator()(const decimal_type_impl& t) {
        if (value.IsString()) {
            return t.from_string(rjson::to_string_view(value));
        } else if (!value.IsNumber()) {
            throw marshal_exception(
                    format("{} must be represented as numeric or string in JSON", value));
        }

        return t.from_string(rjson::print(value));
    }
    bytes operator()(const counter_type_impl& t) {
        if (!value.IsNumber()) {
            throw marshal_exception("Counters must be represented as JSON integer");
        }
        return counter_cell_view::total_value_type()->decompose(to_int64_t(value));
    }
    bytes operator()(const duration_type_impl& t) {
        if (!value.IsString()) {
            throw marshal_exception(format("{} must be represented as string in JSON", value));
        }
        return t.from_string(rjson::to_string_view(value));
    }
    bytes operator()(const empty_type_impl& t) { return bytes(); }
    bytes operator()(const map_type_impl& t) { return from_json_object_aux(t, value, sf); }
    bytes operator()(const set_type_impl& t) { return from_json_object_aux(t, value, sf); }
    bytes operator()(const list_type_impl& t) { return from_json_object_aux(t, value, sf); }
    bytes operator()(const tuple_type_impl& t) { return from_json_object_aux(t, value, sf); }
    bytes operator()(const user_type_impl& t) { return from_json_object_aux(t, value, sf); }
};
}

bytes from_json_object(const abstract_type& t, const rjson::value& value, cql_serialization_format sf) {
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
    managed_bytes_view mbv(bv);
    std::for_each(llpdi::begin(mbv, sf), llpdi::end(mbv, sf), [&first, &out, &t] (const managed_bytes_view& e) {
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
    managed_bytes_view mbv(bv);
    std::for_each(llpdi::begin(mbv, sf), llpdi::end(mbv, sf), [&first, &out, &t] (const managed_bytes_view& e) {
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

sstring to_json_string(const abstract_type& t, const managed_bytes_view& mbv) {
    return visit(t, to_json_string_visitor{linearized(mbv)});
}
