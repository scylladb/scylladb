/*
 * Copyright (C) 2017 ScyllaDB
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

#include "castas_fcts.hh"
#include "concrete_types.hh"
#include "utils/UUID_gen.hh"
#include "cql3/functions/native_scalar_function.hh"
#include "utils/date.h"
#include <boost/date_time/posix_time/posix_time.hpp>

namespace cql3 {
namespace functions {

namespace {

using bytes_opt = std::optional<bytes>;

class castas_function_for : public cql3::functions::native_scalar_function {
    cql3::functions::castas_fctn _func;
public:
    castas_function_for(data_type to_type,
                        data_type from_type,
                        castas_fctn func)
            : native_scalar_function("castas" + to_type->as_cql3_type().to_string(), to_type, {from_type})
            , _func(func) {
    }
    virtual bool is_pure() const override {
        return true;
    }
    virtual void print(std::ostream& os) const override {
        os << "cast(" << _arg_types[0]->name() << " as " << _return_type->name() << ")";
    }
    virtual bytes_opt execute(cql_serialization_format sf, const std::vector<bytes_opt>& parameters) override {
        auto from_type = arg_types()[0];
        auto to_type = return_type();

        auto&& val = parameters[0];
        if (!val) {
            return val;
        }
        auto val_from = from_type->deserialize(*val);
        auto val_to = _func(val_from);
        return to_type->decompose(val_to);
    }
};

shared_ptr<function> make_castas_function(data_type to_type, data_type from_type, castas_fctn func) {
    return ::make_shared<castas_function_for>(std::move(to_type), std::move(from_type), std::move(func));
}

} /* Anonymous Namespace */

/*
 * Support for CAST(. AS .) functions.
 */
namespace {

using bytes_opt = std::optional<bytes>;

template<typename ToType, typename FromType>
std::function<data_value(data_value)> make_castas_fctn_simple() {
    return [](data_value from) -> data_value {
        auto val_from = value_cast<FromType>(from);
        return static_cast<ToType>(val_from);
    };
}

template<typename ToType>
std::function<data_value(data_value)> make_castas_fctn_from_decimal_to_float() {
    return [](data_value from) -> data_value {
        auto val_from = value_cast<big_decimal>(from);
        return static_cast<ToType>(val_from.as_rational());
    };
}

static utils::multiprecision_int from_decimal_to_cppint(const data_value& from) {
    const auto& val_from = value_cast<big_decimal>(from);
    auto r = val_from.as_rational();
    return utils::multiprecision_int(numerator(r)/denominator(r));
}

template<typename ToType>
std::function<data_value(data_value)> make_castas_fctn_from_varint_to_integer() {
    return [](data_value from) -> data_value {
        const auto& varint = value_cast<utils::multiprecision_int>(from);
        return static_cast<ToType>(from_varint_to_integer(varint));
    };
}

template<typename ToType>
std::function<data_value(data_value)> make_castas_fctn_from_decimal_to_integer() {
    return [](data_value from) -> data_value {
        auto varint = from_decimal_to_cppint(from);
        return static_cast<ToType>(from_varint_to_integer(varint));
    };
}

std::function<data_value(data_value)> make_castas_fctn_from_decimal_to_varint() {
    return [](data_value from) -> data_value {
        return from_decimal_to_cppint(from);
    };
}

template<typename FromType>
std::function<data_value(data_value)> make_castas_fctn_from_integer_to_decimal() {
    return [](data_value from) -> data_value {
        auto val_from = value_cast<FromType>(from);
        return big_decimal(1, 10*static_cast<boost::multiprecision::cpp_int>(val_from));
    };
}

template<typename FromType>
std::function<data_value(data_value)> make_castas_fctn_from_float_to_decimal() {
    return [](data_value from) -> data_value {
        auto val_from = value_cast<FromType>(from);
        return big_decimal(boost::lexical_cast<std::string>(val_from));
    };
}

template<typename FromType>
std::function<data_value(data_value)> make_castas_fctn_to_string() {
    return [](data_value from) -> data_value {
        return to_sstring(value_cast<FromType>(from));
    };
}

std::function<data_value(data_value)> make_castas_fctn_from_varint_to_string() {
    return [](data_value from) -> data_value {
        return to_sstring(value_cast<utils::multiprecision_int>(from).str());
    };
}

std::function<data_value(data_value)> make_castas_fctn_from_decimal_to_string() {
    return [](data_value from) -> data_value {
        return value_cast<big_decimal>(from).to_string();
    };
}

db_clock::time_point millis_to_time_point(const int64_t millis) {
    return db_clock::time_point{std::chrono::milliseconds(millis)};
}

simple_date_native_type time_point_to_date(const db_clock::time_point& tp) {
    const auto epoch = boost::posix_time::from_time_t(0);
    auto timestamp = tp.time_since_epoch().count();
    auto time = boost::posix_time::from_time_t(0) + boost::posix_time::milliseconds(timestamp);
    const auto diff = time.date() - epoch.date();
    return simple_date_native_type{uint32_t(diff.days() + (1UL<<31))};
}

db_clock::time_point date_to_time_point(const uint32_t date) {
    const auto epoch = boost::posix_time::from_time_t(0);
    const auto target_date = epoch + boost::gregorian::days(int64_t(date) - (1UL<<31));
    boost::posix_time::time_duration duration = target_date - epoch;
    const auto millis = std::chrono::milliseconds(duration.total_milliseconds());
    return db_clock::time_point(std::chrono::duration_cast<db_clock::duration>(millis));
}

std::function<data_value(data_value)> make_castas_fctn_from_timestamp_to_date() {
    return [](data_value from) -> data_value {
        const auto val_from = value_cast<db_clock::time_point>(from);
        return time_point_to_date(val_from);
    };
}

std::function<data_value(data_value)> make_castas_fctn_from_date_to_timestamp() {
    return [](data_value from) -> data_value {
        const auto val_from = value_cast<uint32_t>(from);
        return date_to_time_point(val_from);
    };
}

std::function<data_value(data_value)> make_castas_fctn_from_timeuuid_to_timestamp() {
    return [](data_value from) -> data_value {
        const auto val_from = value_cast<utils::UUID>(from);
        return db_clock::time_point{db_clock::duration{utils::UUID_gen::unix_timestamp(val_from)}};
    };
}

std::function<data_value(data_value)> make_castas_fctn_from_timeuuid_to_date() {
    return [](data_value from) -> data_value {
        const auto val_from = value_cast<utils::UUID>(from);
        return time_point_to_date(millis_to_time_point(utils::UUID_gen::unix_timestamp(val_from)));
    };
}

static std::function<data_value(data_value)> make_castas_fctn_from_dv_to_string() {
    return [](data_value from) -> data_value {
        return from.type()->to_string_impl(from);
    };
}

// FIXME: Add conversions for counters, after they are fully implemented...

// Map <ToType, FromType> -> castas_fctn
using castas_fctn_key = std::pair<data_type, data_type>;
struct castas_fctn_hash {
    std::size_t operator()(const castas_fctn_key& x) const noexcept {
        return boost::hash_value(x);
    }
};
using castas_fctns_map = std::unordered_map<castas_fctn_key, castas_fctn, castas_fctn_hash>;

// List of supported castas functions...
thread_local castas_fctns_map castas_fctns {
    { {byte_type, byte_type}, make_castas_fctn_simple<int8_t, int8_t>() },
    { {byte_type, short_type}, make_castas_fctn_simple<int8_t, int16_t>() },
    { {byte_type, int32_type}, make_castas_fctn_simple<int8_t, int32_t>() },
    { {byte_type, long_type}, make_castas_fctn_simple<int8_t, int64_t>() },
    { {byte_type, float_type}, make_castas_fctn_simple<int8_t, float>() },
    { {byte_type, double_type}, make_castas_fctn_simple<int8_t, double>() },
    { {byte_type, varint_type}, make_castas_fctn_from_varint_to_integer<int8_t>() },
    { {byte_type, decimal_type}, make_castas_fctn_from_decimal_to_integer<int8_t>() },

    { {short_type, byte_type}, make_castas_fctn_simple<int16_t, int8_t>() },
    { {short_type, short_type}, make_castas_fctn_simple<int16_t, int16_t>() },
    { {short_type, int32_type}, make_castas_fctn_simple<int16_t, int32_t>() },
    { {short_type, long_type}, make_castas_fctn_simple<int16_t, int64_t>() },
    { {short_type, float_type}, make_castas_fctn_simple<int16_t, float>() },
    { {short_type, double_type}, make_castas_fctn_simple<int16_t, double>() },
    { {short_type, varint_type}, make_castas_fctn_from_varint_to_integer<int16_t>() },
    { {short_type, decimal_type}, make_castas_fctn_from_decimal_to_integer<int16_t>() },

    { {int32_type, byte_type}, make_castas_fctn_simple<int32_t, int8_t>() },
    { {int32_type, short_type}, make_castas_fctn_simple<int32_t, int16_t>() },
    { {int32_type, int32_type}, make_castas_fctn_simple<int32_t, int32_t>() },
    { {int32_type, long_type}, make_castas_fctn_simple<int32_t, int64_t>() },
    { {int32_type, float_type}, make_castas_fctn_simple<int32_t, float>() },
    { {int32_type, double_type}, make_castas_fctn_simple<int32_t, double>() },
    { {int32_type, varint_type}, make_castas_fctn_from_varint_to_integer<int32_t>() },
    { {int32_type, decimal_type}, make_castas_fctn_from_decimal_to_integer<int32_t>() },

    { {long_type, byte_type}, make_castas_fctn_simple<int64_t, int8_t>() },
    { {long_type, short_type}, make_castas_fctn_simple<int64_t, int16_t>() },
    { {long_type, int32_type}, make_castas_fctn_simple<int64_t, int32_t>() },
    { {long_type, long_type}, make_castas_fctn_simple<int64_t, int64_t>() },
    { {long_type, float_type}, make_castas_fctn_simple<int64_t, float>() },
    { {long_type, double_type}, make_castas_fctn_simple<int64_t, double>() },
    { {long_type, varint_type}, make_castas_fctn_from_varint_to_integer<int64_t>() },
    { {long_type, decimal_type}, make_castas_fctn_from_decimal_to_integer<int64_t>() },

    { {float_type, byte_type}, make_castas_fctn_simple<float, int8_t>() },
    { {float_type, short_type}, make_castas_fctn_simple<float, int16_t>() },
    { {float_type, int32_type}, make_castas_fctn_simple<float, int32_t>() },
    { {float_type, long_type}, make_castas_fctn_simple<float, int64_t>() },
    { {float_type, float_type}, make_castas_fctn_simple<float, float>() },
    { {float_type, double_type}, make_castas_fctn_simple<float, double>() },
    { {float_type, varint_type}, make_castas_fctn_simple<float, utils::multiprecision_int>() },
    { {float_type, decimal_type}, make_castas_fctn_from_decimal_to_float<float>() },

    { {double_type, byte_type}, make_castas_fctn_simple<double, int8_t>() },
    { {double_type, short_type}, make_castas_fctn_simple<double, int16_t>() },
    { {double_type, int32_type}, make_castas_fctn_simple<double, int32_t>() },
    { {double_type, long_type}, make_castas_fctn_simple<double, int64_t>() },
    { {double_type, float_type}, make_castas_fctn_simple<double, float>() },
    { {double_type, double_type}, make_castas_fctn_simple<double, double>() },
    { {double_type, varint_type}, make_castas_fctn_simple<double, utils::multiprecision_int>() },
    { {double_type, decimal_type}, make_castas_fctn_from_decimal_to_float<double>() },

    { {varint_type, byte_type}, make_castas_fctn_simple<utils::multiprecision_int, int8_t>() },
    { {varint_type, short_type}, make_castas_fctn_simple<utils::multiprecision_int, int16_t>() },
    { {varint_type, int32_type}, make_castas_fctn_simple<utils::multiprecision_int, int32_t>() },
    { {varint_type, long_type}, make_castas_fctn_simple<utils::multiprecision_int, int64_t>() },
    { {varint_type, float_type}, make_castas_fctn_simple<utils::multiprecision_int, float>() },
    { {varint_type, double_type}, make_castas_fctn_simple<utils::multiprecision_int, double>() },
    { {varint_type, varint_type}, make_castas_fctn_simple<utils::multiprecision_int, utils::multiprecision_int>() },
    { {varint_type, decimal_type}, make_castas_fctn_from_decimal_to_varint() },

    { {decimal_type, byte_type}, make_castas_fctn_from_integer_to_decimal<int8_t>() },
    { {decimal_type, short_type}, make_castas_fctn_from_integer_to_decimal<int16_t>() },
    { {decimal_type, int32_type}, make_castas_fctn_from_integer_to_decimal<int32_t>() },
    { {decimal_type, long_type}, make_castas_fctn_from_integer_to_decimal<int64_t>() },
    { {decimal_type, float_type}, make_castas_fctn_from_float_to_decimal<float>() },
    { {decimal_type, double_type}, make_castas_fctn_from_float_to_decimal<double>() },
    { {decimal_type, varint_type}, make_castas_fctn_from_integer_to_decimal<utils::multiprecision_int>() },
    { {decimal_type, decimal_type}, make_castas_fctn_simple<big_decimal, big_decimal>() },

    { {ascii_type, byte_type}, make_castas_fctn_to_string<int8_t>() },
    { {ascii_type, short_type}, make_castas_fctn_to_string<int16_t>() },
    { {ascii_type, int32_type}, make_castas_fctn_to_string<int32_t>() },
    { {ascii_type, long_type}, make_castas_fctn_to_string<int64_t>() },
    { {ascii_type, float_type}, make_castas_fctn_to_string<float>() },
    { {ascii_type, double_type}, make_castas_fctn_to_string<double>() },
    { {ascii_type, varint_type}, make_castas_fctn_from_varint_to_string() },
    { {ascii_type, decimal_type}, make_castas_fctn_from_decimal_to_string() },

    { {utf8_type, byte_type}, make_castas_fctn_to_string<int8_t>() },
    { {utf8_type, short_type}, make_castas_fctn_to_string<int16_t>() },
    { {utf8_type, int32_type}, make_castas_fctn_to_string<int32_t>() },
    { {utf8_type, long_type}, make_castas_fctn_to_string<int64_t>() },
    { {utf8_type, float_type}, make_castas_fctn_to_string<float>() },
    { {utf8_type, double_type}, make_castas_fctn_to_string<double>() },
    { {utf8_type, varint_type}, make_castas_fctn_from_varint_to_string() },
    { {utf8_type, decimal_type}, make_castas_fctn_from_decimal_to_string() },

    { {simple_date_type, timestamp_type}, make_castas_fctn_from_timestamp_to_date() },
    { {simple_date_type, timeuuid_type}, make_castas_fctn_from_timeuuid_to_date() },

    { {timestamp_type, simple_date_type}, make_castas_fctn_from_date_to_timestamp() },
    { {timestamp_type, timeuuid_type}, make_castas_fctn_from_timeuuid_to_timestamp() },

    { {ascii_type, timestamp_type}, make_castas_fctn_from_dv_to_string() },
    { {ascii_type, simple_date_type}, make_castas_fctn_from_dv_to_string() },
    { {ascii_type, time_type}, make_castas_fctn_from_dv_to_string() },
    { {ascii_type, timeuuid_type}, make_castas_fctn_from_dv_to_string() },
    { {ascii_type, uuid_type}, make_castas_fctn_from_dv_to_string() },
    { {ascii_type, boolean_type}, make_castas_fctn_from_dv_to_string() },
    { {ascii_type, inet_addr_type}, make_castas_fctn_from_dv_to_string() },
    { {ascii_type, ascii_type}, make_castas_fctn_simple<sstring, sstring>() },

    { {utf8_type, timestamp_type}, make_castas_fctn_from_dv_to_string() },
    { {utf8_type, simple_date_type}, make_castas_fctn_from_dv_to_string() },
    { {utf8_type, time_type}, make_castas_fctn_from_dv_to_string() },
    { {utf8_type, timeuuid_type}, make_castas_fctn_from_dv_to_string() },
    { {utf8_type, uuid_type}, make_castas_fctn_from_dv_to_string() },
    { {utf8_type, boolean_type}, make_castas_fctn_from_dv_to_string() },
    { {utf8_type, inet_addr_type}, make_castas_fctn_from_dv_to_string() },
    { {utf8_type, ascii_type}, make_castas_fctn_simple<sstring, sstring>() },
    { {utf8_type, utf8_type}, make_castas_fctn_simple<sstring, sstring>() },
};

} /* Anonymous Namespace */

castas_fctn get_castas_fctn(data_type to_type, data_type from_type) {
    auto it_candidate = castas_fctns.find(castas_fctn_key{to_type, from_type});
    if (it_candidate == castas_fctns.end()) {
        throw exceptions::invalid_request_exception(format("{} cannot be cast to {}", from_type->name(), to_type->name()));
    }

    return it_candidate->second;
}

shared_ptr<function> castas_functions::get(data_type to_type, const std::vector<shared_ptr<cql3::selection::selector>>& provided_args) {
    if (provided_args.size() != 1) {
        throw exceptions::invalid_request_exception("Invalid CAST expression");
    }
    auto from_type = provided_args[0]->get_type();
    auto from_type_key = from_type;
    if (from_type_key->is_reversed()) {
        from_type_key = dynamic_cast<const reversed_type_impl&>(*from_type).underlying_type();
    }

    auto f = get_castas_fctn(to_type, from_type_key);
    return make_castas_function(to_type, from_type, f);
}

}
}
