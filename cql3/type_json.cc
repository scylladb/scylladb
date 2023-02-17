/*
 * Copyright (C) 2019-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
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
#include <bit>
#include <compare>
#include <limits>
#include <utility>
#include <boost/algorithm/string/trim_all.hpp>
#include <boost/algorithm/string.hpp>

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

template <std::floating_point Tf, std::integral Ti>
static std::partial_ordering cmp(Tf x, Ti y) {
    return x <=> y;
}

// With IEEE-754, a 64-bit float point number is represented with
// - 1 sign bit
// - 11 bits of exponent
// - 52 bits of significand
//
// At the two ends of the range of the numbers represented by a 64 bit float
// point number, the exponent is greater or equal to 1, so the float point numbers
// are not able to represent all integers any more. Specifically, two nearest
// representable integers greater than 2^53 would be have a spacing greater or
// equal to 2. And, we cannot assume the existence of float128, we are not able
// to fit an uint64_t or a 64 bit float into a wider numeric type for comparing
// them, we need to develop an algorithem to tackle the problem of comparing a
// float point number and a fixed point number, where the fixed point number is
// so large that the former is not able to represent it accurately, because the
// number of bits of float point number's significand is not enough for represent
// the all the bits of a two's complement number.
//
// here is a hypothetical example, assuming we need to compare 4.0 with 3, and a
// 64-bit float point number is not able to represent 3, the nearest float point
// number to "3" is "2.0". the hypothetical distribution of IEEE-754 float point
// between 2 and 6 is like:
//            |<- 2 ->|                |<-- 8 -->|
// float:   2.0     4.0     6.0 ...  248.0     256.0    264.0
// integer: 2   3   4   5   6   ...          254
// ----------------------------------->
// so, if we cast 3 to a float point number, we'd get 2.0. so we can compare two
// float point numbers: 2.0 < 4.0. in this case, we can safely conclude the
// comparison with the result of "3 < 4.0".
//
// what if we need to compare 2.0 with 3? as usual, we cast 3 to a float point
// number, and get "2.0". but we cannot just go the conclusion of "2.0 == 3" as
// of yet. because this could be caused by rounding! so we just cast "2.0" back
// to the nearest integer, which is "2", then compare "2" with the original "3"
// to see if the nearest rounding changed the value of the integer being
// compared. it turns out "2" is less than the orignal "3", after it is
// casted. so, we can tell that 3 was rounded to 2.0, and it is not equal to
// 2.0. then we can now conclude the comparison now: "2.0 < 3".
//
// please note, the example above uses 2.0, 3, 4.0 for the sake of simplicy.
// a real world example could be 9007199254740992.0, 9007199254740993,
// 9007199254740994.0, which are 2^53, 2^53+1 and 2^53+2 respectively.
//
// JuliaLang is using a similar algorithm for comparison large float and large integers,
// see https://github.com/JuliaLang/julia/blob/49b361aa2ec68efe4cb0d5870e772eb3e0d89993/base/float.jl#L553-L565
template <std::floating_point Tf, std::integral Ti>
requires (sizeof(Tf) <= 8 && sizeof(Ti) >= 8)
static std::partial_ordering cmp(Tf x, Ti y) {
    // fy is the nearest float which can be represented by Tf, and it is not always
    // accurate enough to be equal to y.
    auto fy = static_cast<Tf>(y);
    // don't go the conclusion that "x == fy" as of yet, in case fy is nearest
    // rounded. but before comparing the truncated fy, let's make sure fy is
    // in the range of representable values of Ti first.
    if (x < fy || (x == fy && (fy < static_cast<Tf>(std::numeric_limits<Ti>::max()) &&
                               static_cast<Ti>(fy) < y))) {
        return std::partial_ordering::less;
    }
    // the number of bits of mantissa of Tf is so small that it is not able to
    // represent (std::numeric_limits<Ti>::max(), which would need "8 * sizeof(Ti)"
    // bits filled with "1" mantissa. so, if both "x == fy" and fy is equal to the
    // rounded value of it, there are two possible cases:
    // 1. y is std::numeric_limits<Ti>::max(), but if this is the case, x would be
    //    have been greater than it. as explained above, x can never be equal to it.
    //    so this never happens.
    // 2. y is rouneded to this number, and y is smaller than fy and x
    if (x > fy || (x == fy && (fy == static_cast<Tf>(std::numeric_limits<Ti>::max()) ||
                               static_cast<Ti>(fy) > y))) {
        return std::partial_ordering::greater;
    }
    return std::partial_ordering::equivalent;
}

template <typename T> static T to_int(const rjson::value& value) {
    int64_t result;

    if (value.IsInt64()) {
        result = value.GetInt64();
    } else if (value.IsInt()) {
        result = value.GetInt();
    } else if (value.IsUint()) {
        result = value.GetUint();
    } else if (value.IsUint64()) {
        uint64_t u64_result = value.GetUint64();

        if (std::cmp_greater(std::numeric_limits<T>::min(), u64_result) 
        || std::cmp_greater(u64_result, std::numeric_limits<T>::max())) {
            throw marshal_exception(format("Value {} out of range", u64_result));
        }
        return u64_result;
    } else if (value.IsDouble()) {
        // We allow specifing integer constants
        // using scientific notation (for example 1.3e8)
        // and floating-point numbers ending with .0 (for example 12.0),
        // but not floating-point numbers with fractional part (12.34).
        //
        // The reason is that JSON standard does not have separate
        // types for integers and floating-point numbers, only
        // a single "number" type. Some serializers may
        // produce an integer in that floating-point format.
        double double_value = value.GetDouble();

        // Check if the value contains disallowed fractional part (.34 from 12.34).
        // With RapidJSON and an integer value in range [-(2^53)+1, (2^53)-1], 
        // the fractional part will be zero as the entire value
        // fits in 53-bit significand. RapidJSON's parsing code does not lose accuracy:
        // when parsing a number like 12.34e8, it accumulates 1234 to a int64_t number,
        // then converts it to double and multiples by power of 10, never having any
        // digit in fractional part.
        double integral;
        double fractional = std::modf(double_value, &integral);
        if (fractional != 0.0 && fractional != -0.0) {
            throw marshal_exception(format("Incorrect JSON floating-point value "
                "for int64 type: {} (it should not contain fractional part {})", value, fractional));
        }

        if (std::is_lt(cmp(double_value, std::numeric_limits<T>::min())) ||
            std::is_gt(cmp(double_value, std::numeric_limits<T>::max()))) {
            throw marshal_exception(format("Value {} out of range", double_value));
        }

        return double_value;
    }
    else {
        throw marshal_exception(format("Incorrect JSON value for int64 type: {}", value));
    }

    if (std::cmp_greater(std::numeric_limits<T>::min(), result) 
        || std::cmp_greater(result, std::numeric_limits<T>::max())) {
        throw marshal_exception(format("Value {} out of range", result));
    }

    return result;
}

static std::string_view validated_to_string_view(const rjson::value& v, const char* type_name) {
    if (!v.IsString()) {
        throw marshal_exception(format("{} must be represented as string in JSON, instead got '{}'", type_name, v));
    }
    return rjson::to_string_view(v); 
}

static bytes from_json_object_aux(const map_type_impl& t, const rjson::value& value) {
    if (!value.IsObject()) {
        throw marshal_exception("map_type must be represented as JSON Object");
    }
    std::map<bytes, bytes, serialized_compare> raw_map(t.get_keys_type()->as_less_comparator());
    for (auto it = value.MemberBegin(); it != value.MemberEnd(); ++it) {
        bytes value = from_json_object(*t.get_values_type(), it->value);
        if (!t.get_keys_type()->is_compatible_with(*utf8_type)) {
            // Keys in maps can only be strings in JSON, but they can also be a string representation
            // of another JSON type, which needs to be reparsed. Example - map<frozen<list<int>>, int>
            // will be represented like this: { "[1, 3, 6]": 3, "[]": 0, "[1, 2]": 2 }
            rjson::value map_key = rjson::parse(rjson::to_string_view(it->name));
            raw_map.emplace(from_json_object(*t.get_keys_type(), map_key), std::move(value));
        } else {
            raw_map.emplace(from_json_object(*t.get_keys_type(), it->name), std::move(value));
        }
    }
    return map_type_impl::serialize_to_bytes(raw_map);
}

static bytes from_json_object_aux(const set_type_impl& t, const rjson::value& value) {
    if (!value.IsArray()) {
        throw marshal_exception("set_type must be represented as JSON Array");
    }
    std::set<bytes, serialized_compare> raw_set(t.get_elements_type()->as_less_comparator());
    for (const rjson::value& v : value.GetArray()) {
        raw_set.emplace(from_json_object(*t.get_elements_type(), v));
    }
    return collection_type_impl::pack(raw_set.begin(), raw_set.end(), raw_set.size());
}

static bytes from_json_object_aux(const list_type_impl& t, const rjson::value& value) {
    std::vector<bytes> values;
    if (!value.IsArray()) {
        throw marshal_exception("list_type must be represented as JSON Array");
    }
    for (const rjson::value& v : value.GetArray()) {
        values.emplace_back(from_json_object(*t.get_elements_type(), v));
    }
    return collection_type_impl::pack(values.begin(), values.end(), values.size());
}

static bytes from_json_object_aux(const tuple_type_impl& t, const rjson::value& value) {
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
        raw_tuple.emplace_back(from_json_object(**ti, *vi));
    }
    return t.build_value(std::move(raw_tuple));
}

static bytes from_json_object_aux(const user_type_impl& ut, const rjson::value& value) {
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
            raw_tuple.push_back(from_json_object(*t, *v));
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
    ;
    bytes operator()(const reversed_type_impl& t) { return from_json_object(*t.underlying_type(), value); }
    template <typename T> bytes operator()(const integer_type_impl<T>& t) {
        if (value.IsString()) {
            return t.from_string(rjson::to_string_view(value));
        }

        return t.decompose(to_int<T>(value));
    }
    bytes operator()(const string_type_impl& t) { return t.from_string(rjson::to_string_view(value)); }
    bytes operator()(const bytes_type_impl& t) {
        std::string_view string_v = validated_to_string_view(value, "bytes_type");
        if (string_v.size() < 2 || string_v[0] != '0' || string_v[1] != 'x') {
            throw marshal_exception("Blob JSON strings must start with 0x");
        }
        string_v.remove_prefix(2);
        return bytes_type->from_string(string_v);
    }
    bytes operator()(const boolean_type_impl& t) {
        if (!value.IsBool()) {
            if (value.IsString()) {
                std::string str(rjson::to_string_view(value));
                boost::trim_all(str);
                boost::to_lower(str);

                if (str == "true") {
                    return t.decompose(true);
                } else if (str == "false") {
                    return t.decompose(false);
                }
            }
            throw marshal_exception(format("Invalid JSON object {}", value));
        }
        return t.decompose(value.GetBool());
    }
    bytes operator()(const timestamp_date_base_class& t) {
        if (!value.IsString() && !value.IsNumber()) {
            throw marshal_exception("timestamp_type must be represented as string or integer");
        }
        if (value.IsNumber()) {
            return long_type->decompose(to_int<int64_t>(value));
        }
        return t.from_string(rjson::to_string_view(value));
    }
    bytes operator()(const timeuuid_type_impl& t) { return t.from_string(validated_to_string_view(value, "timeuuid_type")); }
    bytes operator()(const simple_date_type_impl& t) { return t.from_string(validated_to_string_view(value, "simple_date_type")); }
    bytes operator()(const time_type_impl& t) { return t.from_string(validated_to_string_view(value, "time_type")); }
    bytes operator()(const uuid_type_impl& t) { return t.from_string(validated_to_string_view(value, "uuid_type")); }
    bytes operator()(const inet_addr_type_impl& t) { return t.from_string(validated_to_string_view(value, "inet_addr_type")); }
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
        return counter_cell_view::total_value_type()->decompose(to_int<int64_t>(value));
    }
    bytes operator()(const duration_type_impl& t) { return t.from_string(validated_to_string_view(value, "duration_type")); }
    bytes operator()(const empty_type_impl& t) { return bytes(); }
    bytes operator()(const map_type_impl& t) { return from_json_object_aux(t, value); }
    bytes operator()(const set_type_impl& t) { return from_json_object_aux(t, value); }
    bytes operator()(const list_type_impl& t) { return from_json_object_aux(t, value); }
    bytes operator()(const tuple_type_impl& t) { return from_json_object_aux(t, value); }
    bytes operator()(const user_type_impl& t) { return from_json_object_aux(t, value); }
};
}

bytes from_json_object(const abstract_type& t, const rjson::value& value) {
    return visit(t, from_json_object_visitor{value});
}

template <typename T> static T compose_value(const integer_type_impl<T>& t, bytes_view bv) {
    if (bv.size() != sizeof(T)) {
        throw marshal_exception(format("Size mismatch for type {}: got {:d} bytes", t.name(), bv.size()));
    }
    return read_be<T>(reinterpret_cast<const char*>(bv.data()));
}

static sstring to_json_string_aux(const map_type_impl& t, bytes_view bv) {
    std::ostringstream out;

    out << '{';
    auto size = read_collection_size(bv);
    for (int i = 0; i < size; ++i) {
        auto kb = read_collection_key(bv);
        auto vb = read_collection_value_nonnull(bv);

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
    out << '[';
    managed_bytes_view mbv(bv);
    std::for_each(llpdi::begin(mbv), llpdi::end(mbv), [&first, &out, &t] (const managed_bytes_view_opt& e) {
        if (first) {
            first = false;
        } else {
            out << ", ";
        }
        if (e) {
            out << to_json_string(*t.get_elements_type(), *e);
        } else {
            // Impossible in sets, but let's not insist here.
            out << "null";
        }
    });
    out << ']';
    return out.str();
}

static sstring to_json_string_aux(const list_type_impl& t, bytes_view bv) {
    using llpdi = listlike_partial_deserializing_iterator;
    std::ostringstream out;
    bool first = true;
    out << '[';
    managed_bytes_view mbv(bv);
    std::for_each(llpdi::begin(mbv), llpdi::end(mbv), [&first, &out, &t] (const managed_bytes_view_opt& e) {
        if (first) {
            first = false;
        } else {
            out << ", ";
        }
        if (e) {
            out << to_json_string(*t.get_elements_type(), *e);
        } else {
            out << "null";
        }
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
