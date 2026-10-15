/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "format_fcts.hh"
#include "cql3/functions/native_scalar_function.hh"
#include "exceptions/exceptions.hh"
#include "marshal_exception.hh"
#include "utils/chunked_string.hh"
#include "utils/multiprecision_int.hh"

#include <array>
#include <optional>
#include <ranges>
#include <span>
#include <string_view>

namespace cql3::functions {

const std::vector<data_type>& format_value_types() {
    // The types are per-shard objects, so the list must be per-shard too.
    static thread_local const std::vector<data_type> types = {
        int32_type, byte_type, short_type, long_type, varint_type, ascii_type, utf8_type
    };
    return types;
}

static sstring to_sstring(const managed_bytes& b) {
    auto bv = to_bytes(b);
    return sstring(reinterpret_cast<const char*>(bv.data()), bv.size());
}

// Parses a string holding an integer the same way CQL parses any bigint
// from a string: an optional sign followed by decimal digits.
static int64_t parse_long(std::string_view s) {
    try {
        return value_cast<int64_t>(long_type->deserialize(long_type->from_string(s)));
    } catch (marshal_exception&) {
        throw exceptions::invalid_request_exception(fmt::format("unable to convert string '{}' to a value of type long", s));
    }
}

// format_bytes() and format_time() both take the arguments
// (value [, source_unit] [, target_unit]), where value - the size or
// duration to format - is an integer of one of format_value_types(),
// possibly written as a string. This function returns the value as an
// int64_t, or nullopt if it is null - and then the function returns null.
// A value which isn't a non-negative integer is an error.
static std::optional<int64_t> get_value(const data_type& type, const managed_bytes_opt& param) {
    if (!param) {
        return std::nullopt;
    }
    int64_t value;
    auto kind = type->get_kind();
    if (kind == abstract_type::kind::ascii || kind == abstract_type::kind::utf8) {
        value = parse_long(to_sstring(*param));
    } else {
        data_value v = type->deserialize(*param);
        if (v.is_null()) {
            return std::nullopt;
        }
        if (kind == abstract_type::kind::int32) {
            value = value_cast<int32_t>(v);
        } else if (kind == abstract_type::kind::byte) {
            value = value_cast<int8_t>(v);
        } else if (kind == abstract_type::kind::short_kind) {
            value = value_cast<int16_t>(v);
        } else if (kind == abstract_type::kind::long_kind) {
            value = value_cast<int64_t>(v);
        } else {
            const auto& i = value_cast<utils::multiprecision_int>(v);
            if (i > std::numeric_limits<int64_t>::max()) {
                throw exceptions::invalid_request_exception("value is too large");
            }
            value = i < 0 ? -1 : static_cast<int64_t>(i);
        }
    }
    if (value < 0) {
        throw exceptions::invalid_request_exception("value must be non-negative");
    }
    return value;
}

// Formats a non-negative number with at most two decimal places, without
// trailing zeros, and never in scientific notation.
static std::string format_number(double v) {
    if (v >= 1e16) {
        // Such a large double is an integer. Like Cassandra, print the
        // shortest digits that identify it, padded with zeros, e.g., 2^63 is
        // printed as 9223372036854776000, not its exact value
        // 9223372036854775808. fmt prints these shortest digits in scientific
        // notation, as 9.223372036854776e+18.
        auto s = fmt::format("{}", v);
        auto e = s.find('e');
        if (e == std::string::npos) {
            return s;
        }
        std::string digits = s.substr(0, e);
        std::erase(digits, '.');
        int exp10 = std::stoi(s.substr(e + 1));
        return digits + std::string(exp10 + 1 - digits.size(), '0');
    }
    // fmt rounds according to the exact binary value of the double. Cassandra
    // does the same, except that for an exact tie it rounds up while fmt
    // rounds to even. An exact tie in the third decimal place is rare - only
    // the fractions .125, .375, .625 and .875 - and for .375 and .875 both
    // ways give the same result.
    auto ret = fmt::format("{:.2f}", v);
    // Drop trailing zeros, and the decimal point if no decimals remain.
    ret.erase(ret.find_last_not_of('0') + 1);
    if (ret.ends_with('.')) {
        ret.pop_back();
    }
    return ret;
}

static managed_bytes_opt make_result(double value, std::string_view unit) {
    return managed_bytes(utf8_type->decompose(sstring(fmt::format("{} {}", format_number(value), unit))));
}

namespace {

struct storage_unit {
    std::string_view symbol;
    int power_of_1024;
};

constexpr std::array<storage_unit, 4> storage_units = {{
    {"B", 0}, {"KiB", 1}, {"MiB", 2}, {"GiB", 3},
}};

struct time_unit {
    std::string_view symbol;
    double nanos;
};

constexpr std::array<time_unit, 7> time_units = {{
    {"ns", 1.0}, {"us", 1'000.0}, {"ms", 1'000'000.0}, {"s", 1'000'000'000.0},
    {"m", 60.0 * 1'000'000'000.0}, {"h", 3600.0 * 1'000'000'000.0}, {"d", 86400.0 * 1'000'000'000.0},
}};

}

static bool equals_ignore_case(std::string_view a, std::string_view b) {
    return std::ranges::equal(a, b, [] (unsigned char x, unsigned char y) { return std::tolower(x) == std::tolower(y); });
}

// Unlike a null value, a null unit is an error.
static sstring get_unit(const managed_bytes_opt& b) {
    if (!b) {
        throw exceptions::invalid_request_exception("none of the arguments may be null");
    }
    return to_sstring(*b);
}

static const storage_unit& get_storage_unit(const managed_bytes_opt& b) {
    auto s = get_unit(b);
    for (const auto& u : storage_units) {
        if (equals_ignore_case(s, u.symbol)) {
            return u;
        }
    }
    throw exceptions::invalid_request_exception(fmt::format("Unsupported data storage unit: {}. Supported units are: B, KiB, MiB, GiB", s));
}

static const time_unit& get_time_unit(const managed_bytes_opt& b) {
    auto s = get_unit(b);
    // "µs", with the micro sign, is another name for "us".
    std::string_view symbol = equals_ignore_case(s, "µs") ? "us" : std::string_view(s);
    for (const auto& u : time_units) {
        if (equals_ignore_case(symbol, u.symbol)) {
            return u;
        }
    }
    throw exceptions::invalid_request_exception(fmt::format("Unsupported time unit: {}. Supported units are: ns, us, ms, s, m, h, d", s));
}

// Converts a size between units. Converting to a larger unit gives a
// fraction, converting to a smaller unit saturates at the maximum int64_t.
static double convert_size(int64_t value, const storage_unit& from, const storage_unit& to) {
    if (to.power_of_1024 >= from.power_of_1024) {
        return double(value) / double(int64_t(1) << (10 * (to.power_of_1024 - from.power_of_1024)));
    }
    int64_t factor = int64_t(1) << (10 * (from.power_of_1024 - to.power_of_1024));
    if (value > std::numeric_limits<int64_t>::max() / factor) {
        return double(std::numeric_limits<int64_t>::max());
    }
    return double(value * factor);
}

shared_ptr<function> make_format_bytes_function(std::vector<data_type> arg_types) {
    auto value_type = arg_types[0];
    return make_native_scalar_function<true>("format_bytes", utf8_type, std::move(arg_types),
            [value_type] (std::span<const managed_bytes_opt> parameters) -> managed_bytes_opt {
        auto value = get_value(value_type, parameters[0]);
        if (!value) {
            return std::nullopt;
        }
        const storage_unit* from = &storage_units[0];
        const storage_unit* to;
        if (parameters.size() == 1) {
            to = &storage_units[0];
            for (const auto& u : storage_units | std::views::drop(1)) {
                if (*value > int64_t(1) << (10 * u.power_of_1024)) {
                    to = &u;
                }
            }
        } else if (parameters.size() == 2) {
            to = &get_storage_unit(parameters[1]);
        } else {
            from = &get_storage_unit(parameters[1]);
            to = &get_storage_unit(parameters[2]);
        }
        return make_result(convert_size(*value, *from, *to), to->symbol);
    });
}

shared_ptr<function> make_format_time_function(std::vector<data_type> arg_types) {
    auto value_type = arg_types[0];
    return make_native_scalar_function<true>("format_time", utf8_type, std::move(arg_types),
            [value_type] (std::span<const managed_bytes_opt> parameters) -> managed_bytes_opt {
        auto value = get_value(value_type, parameters[0]);
        if (!value) {
            return std::nullopt;
        }
        if (parameters.size() == 1) {
            // Pick the largest unit, from seconds to days, which is not
            // larger than the value (in milliseconds).
            for (const auto& u : time_units | std::views::reverse | std::views::take(4)) {
                auto ms_per_unit = int64_t(u.nanos / 1'000'000);
                if (*value >= ms_per_unit) {
                    return make_result(double(*value) / ms_per_unit, u.symbol);
                }
            }
            return make_result(double(*value), "ms");
        }
        const time_unit& from = parameters.size() == 2 ? time_units[2] : get_time_unit(parameters[1]);
        const time_unit& to = get_time_unit(parameters.back());
        // The result uses the target unit as the user wrote it, not the
        // standard spelling in time_units.
        return make_result(double(*value) * (from.nanos / to.nanos), get_unit(parameters.back()));
    });
}

}
