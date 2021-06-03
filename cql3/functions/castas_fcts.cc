/*
 * Copyright (C) 2017-present ScyllaDB
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
    return ::make_shared<castas_function_for>(std::move(to_type), std::move(from_type), func);
}

} /* Anonymous Namespace */

/*
 * Support for CAST(. AS .) functions.
 */
namespace {

static data_value identity_castas_fctn(data_value val) {
    return val;
}

using bytes_opt = std::optional<bytes>;

template<typename ToType, typename FromType>
static data_value castas_fctn_simple(data_value from) {
    auto val_from = value_cast<FromType>(from);
    return static_cast<ToType>(val_from);
}

template<typename ToType>
static data_value castas_fctn_from_decimal_to_float(data_value from) {
    auto val_from = value_cast<big_decimal>(from);
    return static_cast<ToType>(val_from.as_rational());
}

static utils::multiprecision_int from_decimal_to_cppint(const data_value& from) {
    const auto& val_from = value_cast<big_decimal>(from);
    auto r = val_from.as_rational();
    return utils::multiprecision_int(numerator(r)/denominator(r));
}

template<typename ToType>
static data_value castas_fctn_from_varint_to_integer(data_value from) {
    const auto& varint = value_cast<utils::multiprecision_int>(from);
    return static_cast<ToType>(from_varint_to_integer(varint));
}

template<typename ToType>
static data_value castas_fctn_from_decimal_to_integer(data_value from) {
    auto varint = from_decimal_to_cppint(from);
    return static_cast<ToType>(from_varint_to_integer(varint));
}

static data_value castas_fctn_from_decimal_to_varint(data_value from) {
    return from_decimal_to_cppint(from);
}

template<typename FromType>
static data_value castas_fctn_from_integer_to_decimal(data_value from) {
    auto val_from = value_cast<FromType>(from);
    return big_decimal(1, 10*static_cast<boost::multiprecision::cpp_int>(val_from));
}

template<typename FromType>
static data_value castas_fctn_from_float_to_decimal(data_value from) {
    auto val_from = value_cast<FromType>(from);
    return big_decimal(boost::lexical_cast<std::string>(val_from));
}

template<typename FromType>
static data_value castas_fctn_to_string(data_value from) {
    return to_sstring(value_cast<FromType>(from));
}

static data_value castas_fctn_from_varint_to_string(data_value from) {
    return to_sstring(value_cast<utils::multiprecision_int>(from).str());
}

static data_value castas_fctn_from_decimal_to_string(data_value from) {
    return value_cast<big_decimal>(from).to_string();
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

static data_value castas_fctn_from_timestamp_to_date(data_value from) {
    const auto val_from = value_cast<db_clock::time_point>(from);
    return time_point_to_date(val_from);
}

static data_value castas_fctn_from_date_to_timestamp(data_value from) {
    const auto val_from = value_cast<uint32_t>(from);
    return date_to_time_point(val_from);
}

static data_value castas_fctn_from_timeuuid_to_timestamp(data_value from) {
    const auto val_from = value_cast<utils::UUID>(from);
    return db_clock::time_point{db_clock::duration{utils::UUID_gen::unix_timestamp(val_from)}};
}

static data_value castas_fctn_from_timeuuid_to_date(data_value from) {
    const auto val_from = value_cast<utils::UUID>(from);
    return time_point_to_date(db_clock::time_point{utils::UUID_gen::unix_timestamp(val_from)});
}

static data_value castas_fctn_from_dv_to_string(data_value from) {
    return from.type()->to_string_impl(from);
}

// FIXME: Add conversions for counters, after they are fully implemented...

static constexpr unsigned next_power_of_2(unsigned val) {
    unsigned ret = 1;
    while (ret <= val) {
        ret *= 2;
    }
    return ret;
}

static constexpr unsigned next_kind_power_of_2 = next_power_of_2(static_cast<unsigned>(abstract_type::kind::last));
static constexpr unsigned cast_switch_case_val(abstract_type::kind A, abstract_type::kind B) {
    return static_cast<unsigned>(A) * next_kind_power_of_2 + static_cast<unsigned>(B);
}
} /* Anonymous Namespace */

castas_fctn get_castas_fctn(data_type to_type, data_type from_type) {
    if (from_type == to_type) {
        // Casting any type to itself doesn't make sense, but it is
        // harmless so allow it instead of reporting a confusing error
        // message about TypeX not being castable to TypeX.
        return identity_castas_fctn;
    }

    using kind = abstract_type::kind;
    switch (cast_switch_case_val(to_type->get_kind(), from_type->get_kind())) {
    case cast_switch_case_val(kind::byte, kind::short_kind):
        return castas_fctn_simple<int8_t, int16_t>;
    case cast_switch_case_val(kind::byte, kind::int32):
        return castas_fctn_simple<int8_t, int32_t>;
    case cast_switch_case_val(kind::byte, kind::long_kind):
        return castas_fctn_simple<int8_t, int64_t>;
    case cast_switch_case_val(kind::byte, kind::float_kind):
        return castas_fctn_simple<int8_t, float>;
    case cast_switch_case_val(kind::byte, kind::double_kind):
        return castas_fctn_simple<int8_t, double>;
    case cast_switch_case_val(kind::byte, kind::varint):
        return castas_fctn_from_varint_to_integer<int8_t>;
    case cast_switch_case_val(kind::byte, kind::decimal):
        return castas_fctn_from_decimal_to_integer<int8_t>;

    case cast_switch_case_val(kind::short_kind, kind::byte):
        return castas_fctn_simple<int16_t, int8_t>;
    case cast_switch_case_val(kind::short_kind, kind::int32):
        return castas_fctn_simple<int16_t, int32_t>;
    case cast_switch_case_val(kind::short_kind, kind::long_kind):
        return castas_fctn_simple<int16_t, int64_t>;
    case cast_switch_case_val(kind::short_kind, kind::float_kind):
        return castas_fctn_simple<int16_t, float>;
    case cast_switch_case_val(kind::short_kind, kind::double_kind):
        return castas_fctn_simple<int16_t, double>;
    case cast_switch_case_val(kind::short_kind, kind::varint):
        return castas_fctn_from_varint_to_integer<int16_t>;
    case cast_switch_case_val(kind::short_kind, kind::decimal):
        return castas_fctn_from_decimal_to_integer<int16_t>;

    case cast_switch_case_val(kind::int32, kind::byte):
        return castas_fctn_simple<int32_t, int8_t>;
    case cast_switch_case_val(kind::int32, kind::short_kind):
        return castas_fctn_simple<int32_t, int16_t>;
    case cast_switch_case_val(kind::int32, kind::long_kind):
        return castas_fctn_simple<int32_t, int64_t>;
    case cast_switch_case_val(kind::int32, kind::float_kind):
        return castas_fctn_simple<int32_t, float>;
    case cast_switch_case_val(kind::int32, kind::double_kind):
        return castas_fctn_simple<int32_t, double>;
    case cast_switch_case_val(kind::int32, kind::varint):
        return castas_fctn_from_varint_to_integer<int32_t>;
    case cast_switch_case_val(kind::int32, kind::decimal):
        return castas_fctn_from_decimal_to_integer<int32_t>;

    case cast_switch_case_val(kind::long_kind, kind::byte):
        return castas_fctn_simple<int64_t, int8_t>;
    case cast_switch_case_val(kind::long_kind, kind::short_kind):
        return castas_fctn_simple<int64_t, int16_t>;
    case cast_switch_case_val(kind::long_kind, kind::int32):
        return castas_fctn_simple<int64_t, int32_t>;
    case cast_switch_case_val(kind::long_kind, kind::float_kind):
        return castas_fctn_simple<int64_t, float>;
    case cast_switch_case_val(kind::long_kind, kind::double_kind):
        return castas_fctn_simple<int64_t, double>;
    case cast_switch_case_val(kind::long_kind, kind::varint):
        return castas_fctn_from_varint_to_integer<int64_t>;
    case cast_switch_case_val(kind::long_kind, kind::decimal):
        return castas_fctn_from_decimal_to_integer<int64_t>;

    case cast_switch_case_val(kind::float_kind, kind::byte):
        return castas_fctn_simple<float, int8_t>;
    case cast_switch_case_val(kind::float_kind, kind::short_kind):
        return castas_fctn_simple<float, int16_t>;
    case cast_switch_case_val(kind::float_kind, kind::int32):
        return castas_fctn_simple<float, int32_t>;
    case cast_switch_case_val(kind::float_kind, kind::long_kind):
        return castas_fctn_simple<float, int64_t>;
    case cast_switch_case_val(kind::float_kind, kind::double_kind):
        return castas_fctn_simple<float, double>;
    case cast_switch_case_val(kind::float_kind, kind::varint):
        return castas_fctn_simple<float, utils::multiprecision_int>;
    case cast_switch_case_val(kind::float_kind, kind::decimal):
        return castas_fctn_from_decimal_to_float<float>;

    case cast_switch_case_val(kind::double_kind, kind::byte):
        return castas_fctn_simple<double, int8_t>;
    case cast_switch_case_val(kind::double_kind, kind::short_kind):
        return castas_fctn_simple<double, int16_t>;
    case cast_switch_case_val(kind::double_kind, kind::int32):
        return castas_fctn_simple<double, int32_t>;
    case cast_switch_case_val(kind::double_kind, kind::long_kind):
        return castas_fctn_simple<double, int64_t>;
    case cast_switch_case_val(kind::double_kind, kind::float_kind):
        return castas_fctn_simple<double, float>;
    case cast_switch_case_val(kind::double_kind, kind::varint):
        return castas_fctn_simple<double, utils::multiprecision_int>;
    case cast_switch_case_val(kind::double_kind, kind::decimal):
        return castas_fctn_from_decimal_to_float<double>;

    case cast_switch_case_val(kind::varint, kind::byte):
        return castas_fctn_simple<utils::multiprecision_int, int8_t>;
    case cast_switch_case_val(kind::varint, kind::short_kind):
        return castas_fctn_simple<utils::multiprecision_int, int16_t>;
    case cast_switch_case_val(kind::varint, kind::int32):
        return castas_fctn_simple<utils::multiprecision_int, int32_t>;
    case cast_switch_case_val(kind::varint, kind::long_kind):
        return castas_fctn_simple<utils::multiprecision_int, int64_t>;
    case cast_switch_case_val(kind::varint, kind::float_kind):
        return castas_fctn_simple<utils::multiprecision_int, float>;
    case cast_switch_case_val(kind::varint, kind::double_kind):
        return castas_fctn_simple<utils::multiprecision_int, double>;
    case cast_switch_case_val(kind::varint, kind::decimal):
        return castas_fctn_from_decimal_to_varint;

    case cast_switch_case_val(kind::decimal, kind::byte):
        return castas_fctn_from_integer_to_decimal<int8_t>;
    case cast_switch_case_val(kind::decimal, kind::short_kind):
        return castas_fctn_from_integer_to_decimal<int16_t>;
    case cast_switch_case_val(kind::decimal, kind::int32):
        return castas_fctn_from_integer_to_decimal<int32_t>;
    case cast_switch_case_val(kind::decimal, kind::long_kind):
        return castas_fctn_from_integer_to_decimal<int64_t>;
    case cast_switch_case_val(kind::decimal, kind::float_kind):
        return castas_fctn_from_float_to_decimal<float>;
    case cast_switch_case_val(kind::decimal, kind::double_kind):
        return castas_fctn_from_float_to_decimal<double>;
    case cast_switch_case_val(kind::decimal, kind::varint):
        return castas_fctn_from_integer_to_decimal<utils::multiprecision_int>;

    case cast_switch_case_val(kind::ascii, kind::byte):
    case cast_switch_case_val(kind::utf8, kind::byte):
        return castas_fctn_to_string<int8_t>;

    case cast_switch_case_val(kind::ascii, kind::short_kind):
    case cast_switch_case_val(kind::utf8, kind::short_kind):
        return castas_fctn_to_string<int16_t>;

    case cast_switch_case_val(kind::ascii, kind::int32):
    case cast_switch_case_val(kind::utf8, kind::int32):
        return castas_fctn_to_string<int32_t>;

    case cast_switch_case_val(kind::ascii, kind::long_kind):
    case cast_switch_case_val(kind::utf8, kind::long_kind):
        return castas_fctn_to_string<int64_t>;

    case cast_switch_case_val(kind::ascii, kind::float_kind):
    case cast_switch_case_val(kind::utf8, kind::float_kind):
        return castas_fctn_to_string<float>;

    case cast_switch_case_val(kind::ascii, kind::double_kind):
    case cast_switch_case_val(kind::utf8, kind::double_kind):
        return castas_fctn_to_string<double>;

    case cast_switch_case_val(kind::ascii, kind::varint):
    case cast_switch_case_val(kind::utf8, kind::varint):
        return castas_fctn_from_varint_to_string;

    case cast_switch_case_val(kind::ascii, kind::decimal):
    case cast_switch_case_val(kind::utf8, kind::decimal):
        return castas_fctn_from_decimal_to_string;

    case cast_switch_case_val(kind::simple_date, kind::timestamp):
        return castas_fctn_from_timestamp_to_date;
    case cast_switch_case_val(kind::simple_date, kind::timeuuid):
        return castas_fctn_from_timeuuid_to_date;

    case cast_switch_case_val(kind::timestamp, kind::simple_date):
        return castas_fctn_from_date_to_timestamp;
    case cast_switch_case_val(kind::timestamp, kind::timeuuid):
        return castas_fctn_from_timeuuid_to_timestamp;

    case cast_switch_case_val(kind::ascii, kind::timestamp):
    case cast_switch_case_val(kind::ascii, kind::simple_date):
    case cast_switch_case_val(kind::ascii, kind::time):
    case cast_switch_case_val(kind::ascii, kind::timeuuid):
    case cast_switch_case_val(kind::ascii, kind::uuid):
    case cast_switch_case_val(kind::ascii, kind::boolean):
    case cast_switch_case_val(kind::ascii, kind::inet):
    case cast_switch_case_val(kind::utf8, kind::timestamp):
    case cast_switch_case_val(kind::utf8, kind::simple_date):
    case cast_switch_case_val(kind::utf8, kind::time):
    case cast_switch_case_val(kind::utf8, kind::timeuuid):
    case cast_switch_case_val(kind::utf8, kind::uuid):
    case cast_switch_case_val(kind::utf8, kind::boolean):
    case cast_switch_case_val(kind::utf8, kind::inet):
        return castas_fctn_from_dv_to_string;
    case cast_switch_case_val(kind::utf8, kind::ascii):
        return castas_fctn_simple<sstring, sstring>;
    }
    throw exceptions::invalid_request_exception(format("{} cannot be cast to {}", from_type->name(), to_type->name()));
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
