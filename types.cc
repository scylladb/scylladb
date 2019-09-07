/*
 * Copyright (C) 2015 ScyllaDB
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

#include <boost/lexical_cast.hpp>
#include <algorithm>
#include <cinttypes>
#include "cql3/cql3_type.hh"
#include "cql3/lists.hh"
#include "cql3/maps.hh"
#include "cql3/sets.hh"
#include "concrete_types.hh"
#include <seastar/core/print.hh>
#include <seastar/net/ip.hh>
#include "utils/serialization.hh"
#include "vint-serialization.hh"
#include "combine.hh"
#include <cmath>
#include <chrono>
#include <sstream>
#include <string>
#include <regex>
#include <boost/iterator/transform_iterator.hpp>
#include <boost/range/adaptor/filtered.hpp>
#include <boost/range/numeric.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/date_time/c_local_time_adjustor.hpp>
#include <boost/locale/encoding_utf.hpp>
#include <boost/multiprecision/cpp_int.hpp>
#include <boost/algorithm/cxx11/any_of.hpp>
#include <seastar/net/inet_address.hh>
#include "utils/big_decimal.hh"
#include "utils/date.h"
#include "utils/utf8.hh"
#include "utils/ascii.hh"
#include "mutation_partition.hh"
#include "json.hh"
#include "compaction_garbage_collector.hh"

#include "types/user.hh"
#include "types/tuple.hh"
#include "types/collection.hh"
#include "types/map.hh"
#include "types/list.hh"
#include "types/set.hh"
#include "types/listlike_partial_deserializing_iterator.hh"

template<typename T>
sstring time_point_to_string(const T& tp)
{
    auto timestamp = tp.time_since_epoch().count();
    auto time = boost::posix_time::from_time_t(0) + boost::posix_time::milliseconds(timestamp);
    return boost::posix_time::to_iso_extended_string(time);
}

sstring simple_date_to_string(const uint32_t days_count) {
    date::days days{days_count - (1UL << 31)};
    date::year_month_day ymd{date::local_days{days}};
    std::ostringstream str;
    str << ymd;
    return str.str();
}

sstring time_to_string(const int64_t nanoseconds_count) {
    std::chrono::nanoseconds nanoseconds{nanoseconds_count};
    auto time = date::make_time(nanoseconds);
    std::ostringstream str;
    str << time;
    return str.str();
}

sstring boolean_to_string(const bool b) {
    return b ? "true" : "false";
}

sstring inet_to_string(const seastar::net::inet_address& addr) {
    std::ostringstream out;
    out << addr;
    return out.str();
}

static const char* byte_type_name      = "org.apache.cassandra.db.marshal.ByteType";
static const char* short_type_name     = "org.apache.cassandra.db.marshal.ShortType";
static const char* int32_type_name     = "org.apache.cassandra.db.marshal.Int32Type";
static const char* long_type_name      = "org.apache.cassandra.db.marshal.LongType";
static const char* ascii_type_name     = "org.apache.cassandra.db.marshal.AsciiType";
static const char* utf8_type_name      = "org.apache.cassandra.db.marshal.UTF8Type";
static const char* bytes_type_name     = "org.apache.cassandra.db.marshal.BytesType";
static const char* boolean_type_name   = "org.apache.cassandra.db.marshal.BooleanType";
static const char* timeuuid_type_name  = "org.apache.cassandra.db.marshal.TimeUUIDType";
static const char* timestamp_type_name = "org.apache.cassandra.db.marshal.TimestampType";
static const char* date_type_name      = "org.apache.cassandra.db.marshal.DateType";
static const char* simple_date_type_name = "org.apache.cassandra.db.marshal.SimpleDateType";
static const char* time_type_name      = "org.apache.cassandra.db.marshal.TimeType";
static const char* uuid_type_name      = "org.apache.cassandra.db.marshal.UUIDType";
static const char* inet_addr_type_name = "org.apache.cassandra.db.marshal.InetAddressType";
static const char* double_type_name    = "org.apache.cassandra.db.marshal.DoubleType";
static const char* float_type_name     = "org.apache.cassandra.db.marshal.FloatType";
static const char* varint_type_name    = "org.apache.cassandra.db.marshal.IntegerType";
static const char* decimal_type_name    = "org.apache.cassandra.db.marshal.DecimalType";
static const char* counter_type_name   = "org.apache.cassandra.db.marshal.CounterColumnType";
static const char* duration_type_name = "org.apache.cassandra.db.marshal.DurationType";
static const char* empty_type_name     = "org.apache.cassandra.db.marshal.EmptyType";

template<typename T>
struct simple_type_traits {
    static constexpr size_t serialized_size = sizeof(T);
    static T read_nonempty(bytes_view v) {
        return read_simple_exactly<T>(v);
    }
};

template<>
struct simple_type_traits<bool> {
    static constexpr size_t serialized_size = 1;
    static bool read_nonempty(bytes_view v) {
        return read_simple_exactly<int8_t>(v) != 0;
    }
};

template<>
struct simple_type_traits<db_clock::time_point> {
    static constexpr size_t serialized_size = sizeof(uint64_t);
    static db_clock::time_point read_nonempty(bytes_view v) {
        return db_clock::time_point(db_clock::duration(read_simple_exactly<int64_t>(v)));
    }
};

template <typename T>
simple_type_impl<T>::simple_type_impl(abstract_type::kind k, sstring name, std::optional<uint32_t> value_length_if_fixed)
    : concrete_type<T>(k, std::move(name), std::move(value_length_if_fixed),
              data::type_info::make_fixed_size(simple_type_traits<T>::serialized_size)) {}

template <typename T>
integer_type_impl<T>::integer_type_impl(
        abstract_type::kind k, sstring name, std::optional<uint32_t> value_length_if_fixed)
    : simple_type_impl<T>(k, name, std::move(value_length_if_fixed)) {}

template <typename T> static T compose_value(const integer_type_impl<T>& t, bytes_view bv) {
    if (bv.size() != sizeof(T)) {
        throw marshal_exception(format("Size mismatch for type {}: got {:d} bytes", t.name(), bv.size()));
    }
    return (T)net::ntoh(*reinterpret_cast<const T*>(bv.data()));
}

template <typename T> static bytes decompose_value(T v) {
    bytes b(bytes::initialized_later(), sizeof(v));
    *reinterpret_cast<T*>(b.begin()) = (T)net::hton(v);
    return b;
}

template <typename T> static T parse_int(const integer_type_impl<T>& t, sstring_view s) {
    try {
        auto value64 = boost::lexical_cast<int64_t>(s.begin(), s.size());
        auto value = static_cast<T>(value64);
        if (value != value64) {
            throw marshal_exception(format("Value out of range for type {}: '{}'", t.name(), s));
        }
        return static_cast<T>(value);
    } catch (const boost::bad_lexical_cast& e) {
        throw marshal_exception(format("Invalid number format '{}'", s));
    }
}

// Note that although byte_type is of a fixed size,
// Cassandra (erroneously) treats it as a variable-size
// so we have to pass disengaged optional for the value size
byte_type_impl::byte_type_impl() : integer_type_impl{kind::byte, byte_type_name, {}} {}

short_type_impl::short_type_impl() : integer_type_impl{kind::short_kind, short_type_name, {}} {}

int32_type_impl::int32_type_impl() : integer_type_impl{kind::int32, int32_type_name, 4} {}

long_type_impl::long_type_impl() : integer_type_impl{kind::long_kind, long_type_name, 8} {}

string_type_impl::string_type_impl(kind k, sstring name)
    : concrete_type(k, name, {}, data::type_info::make_variable_size()) {}

ascii_type_impl::ascii_type_impl() : string_type_impl(kind::ascii, ascii_type_name) {}

utf8_type_impl::utf8_type_impl() : string_type_impl(kind::utf8, utf8_type_name) {}

bytes_type_impl::bytes_type_impl()
    : concrete_type(kind::bytes, bytes_type_name, {}, data::type_info::make_variable_size()) {}

boolean_type_impl::boolean_type_impl() : simple_type_impl<bool>(kind::boolean, boolean_type_name, 1) {}

date_type_impl::date_type_impl() : concrete_type(kind::date, date_type_name, 8, data::type_info::make_fixed_size(sizeof(uint64_t))) {}

timeuuid_type_impl::timeuuid_type_impl()
    : concrete_type<utils::UUID>(
              kind::timeuuid, timeuuid_type_name, 16, data::type_info::make_fixed_size(sizeof(uint64_t) * 2)) {}

static int timeuuid_compare_bytes(bytes_view o1, bytes_view o2) {
    auto compare_pos = [&] (unsigned pos, int mask, int ifequal) {
        int d = (o1[pos] & mask) - (o2[pos] & mask);
        return d ? d : ifequal;
    };
    return compare_pos(6, 0xf,
        compare_pos(7, 0xff,
            compare_pos(4, 0xff,
                compare_pos(5, 0xff,
                    compare_pos(0, 0xff,
                        compare_pos(1, 0xff,
                            compare_pos(2, 0xff,
                                compare_pos(3, 0xff, 0))))))));
}

timestamp_type_impl::timestamp_type_impl() : simple_type_impl(kind::timestamp, timestamp_type_name, 8) {}

static boost::posix_time::ptime get_time(const std::smatch& sm) {
    // Unfortunately boost::date_time  parsers are more strict with regards
    // to the expected date format than we need to be.
    auto year = boost::lexical_cast<int>(sm[1]);
    auto month = boost::lexical_cast<int>(sm[2]);
    auto day = boost::lexical_cast<int>(sm[3]);
    boost::gregorian::date date(year, month, day);

    auto hour = sm[5].length() ? boost::lexical_cast<int>(sm[5]) : 0;
    auto minute = sm[6].length() ? boost::lexical_cast<int>(sm[6]) : 0;
    auto second = sm[8].length() ? boost::lexical_cast<int>(sm[8]) : 0;
    boost::posix_time::time_duration time(hour, minute, second);

    if (sm[10].length()) {
        static constexpr auto milliseconds_string_length = 3;
        auto length = sm[10].length();
        if (length > milliseconds_string_length) {
            throw marshal_exception(format("Milliseconds length exceeds expected ({:d})", length));
        }
        auto value = boost::lexical_cast<int>(sm[10]);
        while (length < milliseconds_string_length) {
            value *= 10;
            length++;
        }
        time += boost::posix_time::milliseconds(value);
    }

    return boost::posix_time::ptime(date, time);
}

static boost::posix_time::time_duration get_utc_offset(const std::string& s) {
    static constexpr const char* formats[] = {
            "%H:%M",
            "%H%M",
    };
    for (auto&& f : formats) {
        auto tif = new boost::posix_time::time_input_facet(f);
        std::istringstream ss(s);
        ss.imbue(std::locale(ss.getloc(), tif));
        auto sign = ss.get();
        boost::posix_time::ptime p;
        ss >> p;
        if (ss.good() && ss.peek() == std::istringstream::traits_type::eof()) {
            return p.time_of_day() * (sign == '-' ? -1 : 1);
        }
    }
    throw marshal_exception("Cannot get UTC offset for a timestamp");
}

static int64_t timestamp_from_string(sstring_view s) {
    try {
        std::string str;
        str.resize(s.size());
        std::transform(s.begin(), s.end(), str.begin(), ::tolower);
        if (str == "now") {
            return db_clock::now().time_since_epoch().count();
        }

        char* end;
        auto v = std::strtoll(s.begin(), &end, 10);
        if (end == s.begin() + s.size()) {
            return v;
        }

        std::regex date_re("^(\\d{4})-(\\d+)-(\\d+)([ tT](\\d+):(\\d+)(:(\\d+)(\\.(\\d+))?)?)?");
        std::smatch dsm;
        if (!std::regex_search(str, dsm, date_re)) {
            throw marshal_exception(format("Unable to parse timestamp from '{}'", str));
        }
        auto t = get_time(dsm);

        auto tz = dsm.suffix().str();
        std::regex tz_re("([\\+-]\\d{2}:?(\\d{2})?)");
        std::smatch tsm;
        if (std::regex_match(tz, tsm, tz_re)) {
            t -= get_utc_offset(tsm.str());
        } else if (tz.empty()) {
            typedef boost::date_time::c_local_adjustor<boost::posix_time::ptime> local_tz;
            // local_tz::local_to_utc(), where are you?
            auto t1 = local_tz::utc_to_local(t);
            auto tz_offset = t1 - t;
            auto t2 = local_tz::utc_to_local(t - tz_offset);
            auto dst_offset = t2 - t;
            t -= tz_offset + dst_offset;
        } else if (tz != "z") {
            throw marshal_exception(format("Unable to parse timezone '{}'", tz));
        }
        return (t - boost::posix_time::from_time_t(0)).total_milliseconds();
    } catch (const marshal_exception& me) {
        throw marshal_exception(format("unable to parse date '{}': {}", s, me.what()));
    } catch (...) {
        throw marshal_exception(format("unable to parse date '{}'", s));
    }
}

simple_date_type_impl::simple_date_type_impl() : simple_type_impl{kind::simple_date, simple_date_type_name, {}} {}

static date::year_month_day get_simple_date_time(const std::smatch& sm) {
    auto year = boost::lexical_cast<long>(sm[1]);
    auto month = boost::lexical_cast<unsigned>(sm[2]);
    auto day = boost::lexical_cast<unsigned>(sm[3]);
    return date::year_month_day{date::year{year}, date::month{month}, date::day{day}};
}
static uint32_t serialize(const std::string& input, int64_t days) {
    if (days < std::numeric_limits<int32_t>::min()) {
        throw marshal_exception(format("Input date {} is less than min supported date -5877641-06-23", input));
    }
    if (days > std::numeric_limits<int32_t>::max()) {
        throw marshal_exception(format("Input date {} is greater than max supported date 5881580-07-11", input));
    }
    days += 1UL << 31;
    return static_cast<uint32_t>(days);
}
static uint32_t days_from_string(sstring_view s) {
    std::string str;
    str.resize(s.size());
    std::transform(s.begin(), s.end(), str.begin(), ::tolower);
    char* end;
    auto v = std::strtoll(s.begin(), &end, 10);
    if (end == s.begin() + s.size()) {
        return v;
    }
    static std::regex date_re("^(-?\\d+)-(\\d+)-(\\d+)");
    std::smatch dsm;
    if (!std::regex_match(str, dsm, date_re)) {
        throw marshal_exception(format("Unable to coerce '{}' to a formatted date (long)", str));
    }
    auto t = get_simple_date_time(dsm);
    return serialize(str, date::local_days(t).time_since_epoch().count());
}

time_type_impl::time_type_impl() : simple_type_impl{kind::time, time_type_name, {}} {}

static int64_t parse_time(sstring_view s) {
    static auto format_error = "Timestamp format must be hh:mm:ss[.fffffffff]";
    auto hours_end = s.find(':');
    if (hours_end == std::string::npos) {
        throw marshal_exception(format_error);
    }
    int64_t hours = std::stol(sstring(s.substr(0, hours_end)));
    if (hours < 0 || hours >= 24) {
        throw marshal_exception(format("Hour out of bounds ({:d}).", hours));
    }
    auto minutes_end = s.find(':', hours_end + 1);
    if (minutes_end == std::string::npos) {
        throw marshal_exception(format_error);
    }
    int64_t minutes = std::stol(sstring(s.substr(hours_end + 1, hours_end - minutes_end)));
    if (minutes < 0 || minutes >= 60) {
        throw marshal_exception(format("Minute out of bounds ({:d}).", minutes));
    }
    auto seconds_end = s.find('.', minutes_end + 1);
    if (seconds_end == std::string::npos) {
        seconds_end = s.length();
    }
    int64_t seconds = std::stol(sstring(s.substr(minutes_end + 1, minutes_end - seconds_end)));
    if (seconds < 0 || seconds >= 60) {
        throw marshal_exception(format("Second out of bounds ({:d}).", seconds));
    }
    int64_t nanoseconds = 0;
    if (seconds_end < s.length()) {
        nanoseconds = std::stol(sstring(s.substr(seconds_end + 1)));
        nanoseconds *= std::pow(10, 9 - (s.length() - (seconds_end + 1)));
        if (nanoseconds < 0 || nanoseconds >= 1000 * 1000 * 1000) {
            throw marshal_exception(format("Nanosecond out of bounds ({:d}).", nanoseconds));
        }
    }
    std::chrono::nanoseconds result{};
    result += std::chrono::hours(hours);
    result += std::chrono::minutes(minutes);
    result += std::chrono::seconds(seconds);
    result += std::chrono::nanoseconds(nanoseconds);
    return result.count();
}

uuid_type_impl::uuid_type_impl()
    : concrete_type(kind::uuid, uuid_type_name, 16, data::type_info::make_fixed_size(sizeof(uint64_t) * 2)) {}

using inet_address = seastar::net::inet_address;

inet_addr_type_impl::inet_addr_type_impl()
    : concrete_type<inet_address>(kind::inet, inet_addr_type_name, {}, data::type_info::make_variable_size()) {}

// Integer of same length of a given type. This is useful because our
// ntoh functions only know how to operate on integers.
template <typename T> struct int_of_size;
template <typename D, typename I> struct int_of_size_impl {
    using dtype = D;
    using itype = I;
    static_assert(sizeof(dtype) == sizeof(itype), "size mismatch");
    static_assert(alignof(dtype) == alignof(itype), "align mismatch");
};
template <> struct int_of_size<double> :
    public int_of_size_impl<double, uint64_t> {};
template <> struct int_of_size<float> :
    public int_of_size_impl<float, uint32_t> {};

template <typename T>
struct float_type_traits {
    static constexpr size_t serialized_size = sizeof(typename int_of_size<T>::itype);
    static double read_nonempty(bytes_view v) {
        union {
            T d;
            typename int_of_size<T>::itype i;
        } x;
        x.i = read_simple_exactly<typename int_of_size<T>::itype>(v);
        return x.d;
    }
};
template<> struct simple_type_traits<float> : public float_type_traits<float> {};
template<> struct simple_type_traits<double> : public float_type_traits<double> {};

template <typename T>
floating_type_impl<T>::floating_type_impl(
        abstract_type::kind k, sstring name, std::optional<uint32_t> value_length_if_fixed)
    : simple_type_impl<T>(k, std::move(name), std::move(value_length_if_fixed)) {}

double_type_impl::double_type_impl() : floating_type_impl{kind::double_kind, double_type_name, 8} {}

float_type_impl::float_type_impl() : floating_type_impl{kind::float_kind, float_type_name, 4} {}

varint_type_impl::varint_type_impl() : concrete_type{kind::varint, varint_type_name, { }, data::type_info::make_variable_size()} { }

decimal_type_impl::decimal_type_impl() : concrete_type{kind::decimal, decimal_type_name, { }, data::type_info::make_variable_size()} { }

counter_type_impl::counter_type_impl()
    : abstract_type{kind::counter, counter_type_name, {}, data::type_info::make_variable_size()} {}

// TODO(jhaberku): Move this to Seastar.
template <size_t... Ts, class Function>
auto generate_tuple_from_index(std::index_sequence<Ts...>, Function&& f) {
    // To ensure that tuple is constructed in the correct order (because the evaluation order of the arguments to
    // `std::make_tuple` is unspecified), use braced initialization  (which does define the order). However, we still
    // need to figure out the type.
    using result_type = decltype(std::make_tuple(f(Ts)...));
    return result_type{f(Ts)...};
}

duration_type_impl::duration_type_impl()
    : concrete_type(kind::duration, duration_type_name, {}, data::type_info::make_variable_size()) {}

using common_counter_type = cql_duration::common_counter_type;
static std::tuple<common_counter_type, common_counter_type, common_counter_type> deserialize_counters(bytes_view v) {
    auto deserialize_and_advance = [&v] (auto&& i) {
        auto len = signed_vint::serialized_size_from_first_byte(v.front());
        const auto d = signed_vint::deserialize(v);
        v.remove_prefix(len);

        if (v.empty() && (i != 2)) {
            throw marshal_exception("Cannot deserialize duration");
        }

        return static_cast<common_counter_type>(d);
    };

    return generate_tuple_from_index(std::make_index_sequence<3>(), std::move(deserialize_and_advance));
}

empty_type_impl::empty_type_impl()
    : abstract_type(kind::empty, empty_type_name, 0, data::type_info::make_fixed_size(0)) {}

namespace {
template <typename Func> struct data_value_visitor {
    const void* v;
    Func& f;
    auto operator()(const empty_type_impl& t) { return f(t, v); }
    auto operator()(const counter_type_impl& t) { return f(t, v); }
    auto operator()(const reversed_type_impl& t) { return f(t, v); }
    template <typename T> auto operator()(const T& t) {
        return f(t, reinterpret_cast<const typename T::native_type*>(v));
    }
};
}

// Given an abstract_type and a void pointer to an object of that
// type, call f with the runtime type of t and v casted to the
// corresponding native type.
// This takes an abstract_type and a void pointer instead of a
// data_value to support reversed_type_impl without requiring that
// each visitor create a new data_value just to recurse.
template <typename Func> static auto visit(const abstract_type& t, const void* v, Func&& f) {
    return ::visit(t, data_value_visitor<Func>{v, f});
}

logging::logger collection_type_impl::_logger("collection_type_impl");
const size_t collection_type_impl::max_elements;

shared_ptr<cql3::column_specification> collection_type_impl::make_collection_receiver(
        shared_ptr<cql3::column_specification> collection, bool is_key) const {
    struct visitor {
        const shared_ptr<cql3::column_specification>& collection;
        bool is_key;
        shared_ptr<cql3::column_specification> operator()(const abstract_type&) { abort(); }
        shared_ptr<cql3::column_specification> operator()(const list_type_impl&) {
            return cql3::lists::value_spec_of(collection);
        }
        shared_ptr<cql3::column_specification> operator()(const map_type_impl&) {
            return is_key ? cql3::maps::key_spec_of(*collection) : cql3::maps::value_spec_of(*collection);
        }
        shared_ptr<cql3::column_specification> operator()(const set_type_impl&) {
            return cql3::sets::value_spec_of(collection);
        }
    };
    return ::visit(*this, visitor{collection, is_key});
}

listlike_collection_type_impl::listlike_collection_type_impl(
        kind k, sstring name, data_type elements, bool is_multi_cell)
    : collection_type_impl(k, name, is_multi_cell), _elements(elements) {}

std::vector<atomic_cell>
collection_type_impl::enforce_limit(std::vector<atomic_cell> cells, int version) const {
    assert(is_multi_cell());
    if (version >= 3 || cells.size() <= max_elements) {
        return cells;
    }
    _logger.error("Detected collection with {} elements, more than the {} limit. Only the first {} elements will be returned to the client. "
            "Please see http://cassandra.apache.org/doc/cql3/CQL.html#collections for more details.", cells.size(), max_elements, max_elements);
    cells.erase(cells.begin() + max_elements, cells.end());
    return cells;
}

bytes
collection_type_impl::serialize_for_native_protocol(std::vector<atomic_cell> cells, int version) const {
    assert(is_multi_cell());
    cells = enforce_limit(std::move(cells), version);
    std::vector<bytes> values = serialized_values(std::move(cells));
    // FIXME: implement
    abort();
    // return CollectionSerializer.pack(values, cells.size(), version);
}

static bool is_compatible_with_aux(const collection_type_impl& t, const abstract_type& previous) {
    if (t.get_kind() != previous.get_kind()) {
        return false;
    }

    auto& cprev = static_cast<const collection_type_impl&>(previous);
    if (t.is_multi_cell() != cprev.is_multi_cell()) {
        return false;
    }

    if (!t.is_multi_cell()) {
        return t.is_compatible_with_frozen(cprev);
    }

    if (!t.name_comparator()->is_compatible_with(*cprev.name_comparator())) {
        return false;
    }

    // the value comparator is only used for Cell values, so sorting doesn't matter
    return t.value_comparator()->is_value_compatible_with(*cprev.value_comparator());
}

static bool is_value_compatible_with_internal_aux(const collection_type_impl& t, const abstract_type& previous) {
    if (t.get_kind() != previous.get_kind()) {
        return false;
    }
    auto& cprev = static_cast<const collection_type_impl&>(previous);
    // for multi-cell collections, compatibility and value-compatibility are the same
    if (t.is_multi_cell() || cprev.is_multi_cell()) {
        return t.is_compatible_with(previous);
    }
    return t.is_value_compatible_with_frozen(cprev);
}

bytes
collection_type_impl::to_value(collection_mutation_view mut, cql_serialization_format sf) const {
  return mut.data.with_linearized([&] (bytes_view bv) {
    return to_value(deserialize_mutation_form(bv), sf);
  });
}

collection_type_impl::mutation
collection_type_impl::mutation_view::materialize(const collection_type_impl& ctype) const {
    collection_type_impl::mutation m;
    m.tomb = tomb;
    m.cells.reserve(cells.size());
    for (auto&& e : cells) {
        m.cells.emplace_back(bytes(e.first.begin(), e.first.end()), atomic_cell(*ctype.value_comparator(), e.second));
    }
    return m;
}


size_t collection_size_len(cql_serialization_format sf) {
    if (sf.using_32_bits_for_collections()) {
        return sizeof(int32_t);
    }
    return sizeof(uint16_t);
}

size_t collection_value_len(cql_serialization_format sf) {
    if (sf.using_32_bits_for_collections()) {
        return sizeof(int32_t);
    }
    return sizeof(uint16_t);
}


int read_collection_size(bytes_view& in, cql_serialization_format sf) {
    if (sf.using_32_bits_for_collections()) {
        return read_simple<int32_t>(in);
    } else {
        return read_simple<uint16_t>(in);
    }
}

void write_collection_size(bytes::iterator& out, int size, cql_serialization_format sf) {
    if (sf.using_32_bits_for_collections()) {
        serialize_int32(out, size);
    } else {
        serialize_int16(out, uint16_t(size));
    }
}

bytes_view read_collection_value(bytes_view& in, cql_serialization_format sf) {
    auto size = sf.using_32_bits_for_collections() ? read_simple<int32_t>(in) : read_simple<uint16_t>(in);
    return read_simple_bytes(in, size);
}

void write_collection_value(bytes::iterator& out, cql_serialization_format sf, bytes_view val_bytes) {
    if (sf.using_32_bits_for_collections()) {
        serialize_int32(out, int32_t(val_bytes.size()));
    } else {
        if (val_bytes.size() > std::numeric_limits<uint16_t>::max()) {
            throw marshal_exception(
                    format("Collection value exceeds the length limit for protocol v{:d}. Collection values are limited to {:d} bytes but {:d} bytes value provided",
                            sf.protocol_version(), std::numeric_limits<uint16_t>::max(), val_bytes.size()));
        }
        serialize_int16(out, uint16_t(val_bytes.size()));
    }
    out = std::copy_n(val_bytes.begin(), val_bytes.size(), out);
}

shared_ptr<const abstract_type> abstract_type::underlying_type() const {
    struct visitor {
        shared_ptr<const abstract_type> operator()(const abstract_type& t) { return t.shared_from_this(); }
        shared_ptr<const abstract_type> operator()(const reversed_type_impl& r) { return r.underlying_type(); }
    };
    return visit(*this, visitor{});
}

bool abstract_type::is_counter() const {
    struct visitor {
        bool operator()(const reversed_type_impl& r) { return r.underlying_type()->is_counter(); }
        bool operator()(const abstract_type&) { return false; }
        bool operator()(const counter_type_impl&) { return true; }
    };
    return visit(*this, visitor{});
}

bool abstract_type::is_collection() const {
    struct visitor {
        bool operator()(const reversed_type_impl& r) { return r.underlying_type()->is_collection(); }
        bool operator()(const abstract_type&) { return false; }
        bool operator()(const collection_type_impl&) { return true; }
    };
    return visit(*this, visitor{});
}

bool abstract_type::is_tuple() const {
    struct visitor {
        bool operator()(const abstract_type&) { return false; }
        bool operator()(const tuple_type_impl&) { return true; }
    };
    return visit(*this, visitor{});
}

bool abstract_type::is_multi_cell() const {
    struct visitor {
        bool operator()(const abstract_type&) { return false; }
        bool operator()(const reversed_type_impl& t) { return t.underlying_type()->is_multi_cell(); }
        bool operator()(const collection_type_impl& c) { return c.is_multi_cell(); }
    };
    return visit(*this, visitor{});
}

bool abstract_type::is_native() const { return !is_collection() && !is_tuple(); }

bool abstract_type::is_string() const {
    struct visitor {
        bool operator()(const abstract_type&) { return false; }
        bool operator()(const string_type_impl&) { return true; }
    };
    return visit(*this, visitor{});
}

template<typename Predicate>
GCC6_CONCEPT(requires CanHandleAllTypes<Predicate>)
static bool find(const abstract_type& t, const Predicate& f) {
    struct visitor {
        const Predicate& f;
        bool operator()(const abstract_type&) { return false; }
        bool operator()(const reversed_type_impl& r) { return find(*r.underlying_type(), f); }
        bool operator()(const tuple_type_impl& t) {
            return boost::algorithm::any_of(t.all_types(), [&] (const data_type& dt) { return find(*dt, f); });
        }
        bool operator()(const map_type_impl& m) { return find(*m.get_keys_type(), f) || find(*m.get_values_type(), f); }
        bool operator()(const listlike_collection_type_impl& l) { return find(*l.get_elements_type(), f); }
    };
    return visit(t, [&](const auto& t) {
        if (f(t)) {
            return true;
        }
        return visitor{f}(t);
    });
}

bool abstract_type::references_duration() const {
    struct visitor {
        bool operator()(const abstract_type&) const { return false; }
        bool operator()(const duration_type_impl&) const { return true; }
    };
    return find(*this, visitor{});
}

bool abstract_type::references_user_type(const sstring& keyspace, const bytes& name) const {
    struct visitor {
        const sstring& keyspace;
        const bytes& name;
        bool operator()(const abstract_type&) const { return false; }
        bool operator()(const user_type_impl& u) const { return u._keyspace == keyspace && u._name == name; }
    };
    return find(*this, visitor{keyspace, name});
}

namespace {
struct is_byte_order_equal_visitor {
    template <typename T> bool operator()(const simple_type_impl<T>&) { return true; }
    bool operator()(const concrete_type<utils::UUID>&) { return true; }
    bool operator()(const abstract_type&) { return false; }
    bool operator()(const reversed_type_impl& t) { return t.underlying_type()->is_byte_order_equal(); }
    bool operator()(const string_type_impl&) { return true; }
    bool operator()(const bytes_type_impl&) { return true; }
    bool operator()(const date_type_impl&) { return true; }
    bool operator()(const inet_addr_type_impl&) { return true; }
    bool operator()(const duration_type_impl&) { return true; }
    // FIXME: origin returns false for list.  Why?
    bool operator()(const set_type_impl& s) { return s.get_elements_type()->is_byte_order_equal(); }
};
}

bool abstract_type::is_byte_order_equal() const { return visit(*this, is_byte_order_equal_visitor{}); }

static bool
check_compatibility(const tuple_type_impl &t, const abstract_type& previous, bool (abstract_type::*predicate)(const abstract_type&) const);

bool abstract_type::is_compatible_with(const abstract_type& previous) const {
    if (this == &previous) {
        return true;
    }
    struct visitor {
        const abstract_type& previous;
        bool operator()(const reversed_type_impl& t) {
            if (previous.is_reversed()) {
                return t.underlying_type()->is_compatible_with(*previous.underlying_type());
            }
            return false;
        }
        bool operator()(const utf8_type_impl&) {
            // Anything that is ascii is also utf8, and they both use bytes comparison
            return previous.is_string();
        }
        bool operator()(const bytes_type_impl&) {
            // Both ascii_type_impl and utf8_type_impl really use bytes comparison and
            // bytesType validate everything, so it is compatible with the former.
            return previous.is_string();
        }
        bool operator()(const date_type_impl& t) {
            if (previous.get_kind() == kind::timestamp) {
                static logging::logger date_logger(date_type_name);
                date_logger.warn("Changing from TimestampType to DateType is allowed, but be wary "
                                 "that they sort differently for pre-unix-epoch timestamps "
                                 "(negative timestamp values) and thus this change will corrupt "
                                 "your data if you have such negative timestamp. There is no "
                                 "reason to switch from DateType to TimestampType except if you "
                                 "were using DateType in the first place and switched to "
                                 "TimestampType by mistake.");
                return true;
            }
            return false;
        }
        bool operator()(const timestamp_type_impl& t) {
            if (previous.get_kind() == kind::date) {
                static logging::logger timestamp_logger(timestamp_type_name);
                timestamp_logger.warn("Changing from DateType to TimestampType is allowed, but be wary "
                                      "that they sort differently for pre-unix-epoch timestamps "
                                      "(negative timestamp values) and thus this change will corrupt "
                                      "your data if you have such negative timestamp. So unless you "
                                      "know that you don't have *any* pre-unix-epoch timestamp you "
                                      "should change back to DateType");
                return true;
            }
            return false;
        }
        bool operator()(const tuple_type_impl& t) {
            return check_compatibility(t, previous, &abstract_type::is_compatible_with);
        }
        bool operator()(const collection_type_impl& t) { return is_compatible_with_aux(t, previous); }
        bool operator()(const abstract_type& t) { return false; }
    };

    return visit(*this, visitor{previous});
}

abstract_type::cql3_kind abstract_type::get_cql3_kind_impl() const {
    struct visitor {
        cql3_kind operator()(const ascii_type_impl&) { return cql3_kind::ASCII; }
        cql3_kind operator()(const byte_type_impl&) { return cql3_kind::TINYINT; }
        cql3_kind operator()(const bytes_type_impl&) { return cql3_kind::BLOB; }
        cql3_kind operator()(const boolean_type_impl&) { return cql3_kind::BOOLEAN; }
        cql3_kind operator()(const counter_type_impl&) { return cql3_kind::COUNTER; }
        cql3_kind operator()(const date_type_impl&) { return cql3_kind::TIMESTAMP; }
        cql3_kind operator()(const decimal_type_impl&) { return cql3_kind::DECIMAL; }
        cql3_kind operator()(const double_type_impl&) { return cql3_kind::DOUBLE; }
        cql3_kind operator()(const duration_type_impl&) { return cql3_kind::DURATION; }
        cql3_kind operator()(const empty_type_impl&) { return cql3_kind::EMPTY; }
        cql3_kind operator()(const float_type_impl&) { return cql3_kind::FLOAT; }
        cql3_kind operator()(const inet_addr_type_impl&) { return cql3_kind::INET; }
        cql3_kind operator()(const int32_type_impl&) { return cql3_kind::INT; }
        cql3_kind operator()(const long_type_impl&) { return cql3_kind::BIGINT; }
        cql3_kind operator()(const short_type_impl&) { return cql3_kind::SMALLINT; }
        cql3_kind operator()(const simple_date_type_impl&) { return cql3_kind::DATE; }
        cql3_kind operator()(const utf8_type_impl&) { return cql3_kind::TEXT; }
        cql3_kind operator()(const time_type_impl&) { return cql3_kind::TIME; }
        cql3_kind operator()(const timestamp_type_impl&) { return cql3_kind::TIMESTAMP; }
        cql3_kind operator()(const timeuuid_type_impl&) { return cql3_kind::TIMEUUID; }
        cql3_kind operator()(const uuid_type_impl&) { return cql3_kind::UUID; }
        cql3_kind operator()(const varint_type_impl&) { return cql3_kind::VARINT; }
        cql3_kind operator()(const reversed_type_impl& r) { return r.underlying_type()->get_cql3_kind_impl(); }
        cql3_kind operator()(const tuple_type_impl&) { assert(0 && "no kind for this type"); }
        cql3_kind operator()(const collection_type_impl&) { assert(0 && "no kind for this type"); }
    };
    return visit(*this, visitor{});
}

cql3::cql3_type abstract_type::as_cql3_type() const {
    return cql3::cql3_type(shared_from_this());
}

abstract_type::cql3_kind_enum_set::prepared abstract_type::get_cql3_kind() const {
    return cql3_kind_enum_set::prepare(get_cql3_kind_impl());
}

static sstring quote_json_string(const sstring& s) {
    return json::value_to_quoted_string(s);
}

static sstring cql3_type_name_impl(const abstract_type& t) {
    struct visitor {
        sstring operator()(const ascii_type_impl&) { return "ascii"; }
        sstring operator()(const boolean_type_impl&) { return "boolean"; }
        sstring operator()(const byte_type_impl&) { return "tinyint"; }
        sstring operator()(const bytes_type_impl&) { return "blob"; }
        sstring operator()(const counter_type_impl&) { return "counter"; }
        sstring operator()(const timestamp_type_impl&) { return "timestamp"; }
        sstring operator()(const date_type_impl&) { return "timestamp"; }
        sstring operator()(const decimal_type_impl&) { return "decimal"; }
        sstring operator()(const double_type_impl&) { return "double"; }
        sstring operator()(const duration_type_impl&) { return "duration"; }
        sstring operator()(const empty_type_impl&) { return "empty"; }
        sstring operator()(const float_type_impl&) { return "float"; }
        sstring operator()(const inet_addr_type_impl&) { return "inet"; }
        sstring operator()(const int32_type_impl&) { return "int"; }
        sstring operator()(const list_type_impl& l) {
            return format("list<{}>", l.get_elements_type()->as_cql3_type());
        }
        sstring operator()(const long_type_impl&) { return "bigint"; }
        sstring operator()(const map_type_impl& m) {
            return format("map<{}, {}>", m.get_keys_type()->as_cql3_type(), m.get_values_type()->as_cql3_type());
        }
        sstring operator()(const reversed_type_impl& r) { return cql3_type_name_impl(*r.underlying_type()); }
        sstring operator()(const set_type_impl& s) { return format("set<{}>", s.get_elements_type()->as_cql3_type()); }
        sstring operator()(const short_type_impl&) { return "smallint"; }
        sstring operator()(const simple_date_type_impl&) { return "date"; }
        sstring operator()(const time_type_impl&) { return "time"; }
        sstring operator()(const timeuuid_type_impl&) { return "timeuuid"; }
        sstring operator()(const tuple_type_impl& t) {
            return format("tuple<{}>", ::join(", ", t.all_types() | boost::adaptors::transformed(std::mem_fn(
                                                                            &abstract_type::as_cql3_type))));
        }
        sstring operator()(const user_type_impl& u) { return u.get_name_as_string(); }
        sstring operator()(const utf8_type_impl&) { return "text"; }
        sstring operator()(const uuid_type_impl&) { return "uuid"; }
        sstring operator()(const varint_type_impl&) { return "varint"; }
    };
    return visit(t, visitor{});
}

const sstring& abstract_type::cql3_type_name() const {
    if (_cql3_type_name.empty()) {
        auto name = cql3_type_name_impl(*this);
        if (!is_native() && !is_multi_cell()) {
            name = "frozen<" + name + ">";
        }
        _cql3_type_name = name;
    }
    return _cql3_type_name;
}

void write_collection_value(bytes::iterator& out, cql_serialization_format sf, data_type type, const data_value& value) {
    size_t val_len = value.serialized_size();

    if (sf.using_32_bits_for_collections()) {
        serialize_int32(out, val_len);
    } else {
        serialize_int16(out, val_len);
    }

    value.serialize(out);
}

map_type
map_type_impl::get_instance(data_type keys, data_type values, bool is_multi_cell) {
    return intern::get_instance(std::move(keys), std::move(values), is_multi_cell);
}

sstring make_map_type_name(data_type keys, data_type values, bool is_multi_cell)
{
    sstring ret = "";
    if (!is_multi_cell) {
        ret = "org.apache.cassandra.db.marshal.FrozenType(";
    }
    ret += "org.apache.cassandra.db.marshal.MapType(" + keys->name() + "," + values->name() + ")";
    if (!is_multi_cell) {
        ret += ")";
    }
    return ret;
}

map_type_impl::map_type_impl(data_type keys, data_type values, bool is_multi_cell)
        : concrete_type(kind::map, make_map_type_name(keys, values, is_multi_cell), is_multi_cell)
        , _keys(std::move(keys))
        , _values(std::move(values)) {
}

data_type
map_type_impl::freeze() const {
    if (_is_multi_cell) {
        return get_instance(_keys, _values, false);
    } else {
        return shared_from_this();
    }
}

bool
map_type_impl::is_compatible_with_frozen(const collection_type_impl& previous) const {
    assert(!_is_multi_cell);
    auto* p = dynamic_cast<const map_type_impl*>(&previous);
    if (!p) {
        return false;
    }
    return _keys->is_compatible_with(*p->_keys)
            && _values->is_compatible_with(*p->_values);
}

bool
map_type_impl::is_value_compatible_with_frozen(const collection_type_impl& previous) const {
    assert(!_is_multi_cell);
    auto* p = dynamic_cast<const map_type_impl*>(&previous);
    if (!p) {
        return false;
    }
    return _keys->is_compatible_with(*p->_keys)
            && _values->is_value_compatible_with(*p->_values);
}

int32_t
map_type_impl::compare_maps(data_type keys, data_type values, bytes_view o1, bytes_view o2) {
    if (o1.empty()) {
        return o2.empty() ? 0 : -1;
    } else if (o2.empty()) {
        return 1;
    }
    auto sf = cql_serialization_format::internal();
    int size1 = read_collection_size(o1, sf);
    int size2 = read_collection_size(o2, sf);
    // FIXME: use std::lexicographical_compare()
    for (int i = 0; i < std::min(size1, size2); ++i) {
        auto k1 = read_collection_value(o1, sf);
        auto k2 = read_collection_value(o2, sf);
        auto cmp = keys->compare(k1, k2);
        if (cmp != 0) {
            return cmp;
        }
        auto v1 = read_collection_value(o1, sf);
        auto v2 = read_collection_value(o2, sf);
        cmp = values->compare(v1, v2);
        if (cmp != 0) {
            return cmp;
        }
    }
    return size1 == size2 ? 0 : (size1 < size2 ? -1 : 1);
}

static size_t map_serialized_size(const map_type_impl::native_type* m) {
    size_t len = collection_size_len(cql_serialization_format::internal());
    size_t psz = collection_value_len(cql_serialization_format::internal());
    for (auto&& kv : *m) {
        len += psz + kv.first.serialized_size();
        len += psz + kv.second.serialized_size();
    }
    return len;
}

void
map_type_impl::serialize(const void* value, bytes::iterator& out, cql_serialization_format sf) const {
    auto& m = from_value(value);
    write_collection_size(out, m.size(), sf);
    for (auto&& kv : m) {
        write_collection_value(out, sf, _keys, kv.first);
        write_collection_value(out, sf, _values, kv.second);
    }
}

data_value
map_type_impl::deserialize(bytes_view in, cql_serialization_format sf) const {
    native_type m;
    auto size = read_collection_size(in, sf);
    for (int i = 0; i < size; ++i) {
        auto kb = read_collection_value(in, sf);
        auto k = _keys->deserialize(kb);
        auto vb = read_collection_value(in, sf);
        auto v = _values->deserialize(vb);
        m.insert(m.end(), std::make_pair(std::move(k), std::move(v)));
    }
    return make_value(std::move(m));
}

static void validate_aux(const map_type_impl& t, bytes_view v, cql_serialization_format sf) {
    auto size = read_collection_size(v, sf);
    for (int i = 0; i < size; ++i) {
        t.get_keys_type()->validate(read_collection_value(v, sf), sf);
        t.get_values_type()->validate(read_collection_value(v, sf), sf);
    }
}

static sstring map_to_string(const std::vector<std::pair<data_value, data_value>>& v, bool include_frozen_type) {
    std::ostringstream out;

    if (include_frozen_type) {
        out << "(";
    }

    out << ::join(", ", v | boost::adaptors::transformed([] (const std::pair<data_value, data_value>& p) {
        std::ostringstream out;
        const auto& k = p.first;
        const auto& v = p.second;
        out << "{" << k.type()->to_string_impl(k) << " : ";
        out << v.type()->to_string_impl(v) << "}";
        return out.str();
    }));

    if (include_frozen_type) {
        out << ")";
    }

    return out.str();
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
        sstring string_key = t.get_keys_type()->to_json_string(kb);
        bool is_unquoted = string_key.empty() || string_key[0] != '"';
        if (is_unquoted) {
            out << '"';
        }
        out << string_key;
        if (is_unquoted) {
            out << '"';
        }
        out << ": ";
        out << t.get_values_type()->to_json_string(vb);
    }
    out << '}';
    return out.str();
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
            raw_map.emplace_back(t.get_keys_type()->from_json_object(map_key, sf));
        } else {
            raw_map.emplace_back(t.get_keys_type()->from_json_object(it.key(), sf));
        }
        raw_map.emplace_back(t.get_values_type()->from_json_object(*it, sf));
    }
    return collection_type_impl::pack(raw_map.begin(), raw_map.end(), raw_map.size() / 2, sf);
}

std::vector<bytes>
map_type_impl::serialized_values(std::vector<atomic_cell> cells) const {
    // FIXME:
    abort();
}

bytes
map_type_impl::to_value(mutation_view mut, cql_serialization_format sf) const {
    std::vector<bytes> linearized;
    std::vector<bytes_view> tmp;
    tmp.reserve(mut.cells.size() * 2);
    for (auto&& e : mut.cells) {
        if (e.second.is_live(mut.tomb, false)) {
            tmp.emplace_back(e.first);
            auto value_view = e.second.value();
            if (value_view.is_fragmented()) {
                auto& v = linearized.emplace_back(value_view.linearize());
                tmp.emplace_back(v);
            } else {
                tmp.emplace_back(value_view.first_fragment());
            }
        }
    }
    return pack(tmp.begin(), tmp.end(), tmp.size() / 2, sf);
}

bytes
map_type_impl::serialize_partially_deserialized_form(
        const std::vector<std::pair<bytes_view, bytes_view>>& v, cql_serialization_format sf) {
    size_t len = collection_value_len(sf) * v.size() * 2 + collection_size_len(sf);
    for (auto&& e : v) {
        len += e.first.size() + e.second.size();
    }
    bytes b(bytes::initialized_later(), len);
    bytes::iterator out = b.begin();

    write_collection_size(out, v.size(), sf);
    for (auto&& e : v) {
        write_collection_value(out, sf, e.first);
        write_collection_value(out, sf, e.second);
    }
    return b;


}

static std::optional<data_type> update_user_type_aux(
        const map_type_impl& m, const shared_ptr<const user_type_impl> updated) {
    auto old_keys = m.get_keys_type();
    auto old_values = m.get_values_type();
    auto k = old_keys->update_user_type(updated);
    auto v = old_values->update_user_type(updated);
    if (!k && !v) {
        return std::nullopt;
    }
    return std::make_optional(static_pointer_cast<const abstract_type>(
        map_type_impl::get_instance(k ? *k : old_keys, v ? *v : old_values, m.is_multi_cell())));
}

auto collection_type_impl::deserialize_mutation_form(bytes_view in) const -> mutation_view {
    mutation_view ret;
    auto has_tomb = read_simple<bool>(in);
    if (has_tomb) {
        auto ts = read_simple<api::timestamp_type>(in);
        auto ttl = read_simple<gc_clock::duration::rep>(in);
        ret.tomb = tombstone{ts, gc_clock::time_point(gc_clock::duration(ttl))};
    }
    auto nr = read_simple<uint32_t>(in);
    ret.cells.reserve(nr);
    for (uint32_t i = 0; i != nr; ++i) {
        // FIXME: we could probably avoid the need for size
        auto ksize = read_simple<uint32_t>(in);
        auto key = read_simple_bytes(in, ksize);
        auto vsize = read_simple<uint32_t>(in);
        // value_comparator(), ugh
        auto value = atomic_cell_view::from_bytes(value_comparator()->imr_state().type_info(), read_simple_bytes(in, vsize));
        ret.cells.emplace_back(key, value);
    }
    assert(in.empty());
    return ret;
}

bool collection_type_impl::is_empty(collection_mutation_view cm) const {
  return cm.data.with_linearized([&] (bytes_view in) { // FIXME: we can guarantee that this is in the first fragment
    auto has_tomb = read_simple<bool>(in);
    return !has_tomb && read_simple<uint32_t>(in) == 0;
  });
}

bool collection_type_impl::is_any_live(collection_mutation_view cm, tombstone tomb, gc_clock::time_point now) const {
  return cm.data.with_linearized([&] (bytes_view in) {
    auto has_tomb = read_simple<bool>(in);
    if (has_tomb) {
        auto ts = read_simple<api::timestamp_type>(in);
        auto ttl = read_simple<gc_clock::duration::rep>(in);
        tomb.apply(tombstone{ts, gc_clock::time_point(gc_clock::duration(ttl))});
    }
    auto nr = read_simple<uint32_t>(in);
    for (uint32_t i = 0; i != nr; ++i) {
        auto ksize = read_simple<uint32_t>(in);
        in.remove_prefix(ksize);
        auto vsize = read_simple<uint32_t>(in);
        auto value = atomic_cell_view::from_bytes(value_comparator()->imr_state().type_info(), read_simple_bytes(in, vsize));
        if (value.is_live(tomb, now, false)) {
            return true;
        }
    }
    return false;
  });
}

api::timestamp_type collection_type_impl::last_update(collection_mutation_view cm) const {
  return cm.data.with_linearized([&] (bytes_view in) {
    api::timestamp_type max = api::missing_timestamp;
    auto has_tomb = read_simple<bool>(in);
    if (has_tomb) {
        max = std::max(max, read_simple<api::timestamp_type>(in));
        (void)read_simple<gc_clock::duration::rep>(in);
    }
    auto nr = read_simple<uint32_t>(in);
    for (uint32_t i = 0; i != nr; ++i) {
        auto ksize = read_simple<uint32_t>(in);
        in.remove_prefix(ksize);
        auto vsize = read_simple<uint32_t>(in);
        auto value = atomic_cell_view::from_bytes(value_comparator()->imr_state().type_info(), read_simple_bytes(in, vsize));
        max = std::max(value.timestamp(), max);
    }
    return max;
  });
}

template <typename Iterator>
collection_mutation
do_serialize_mutation_form(
        const collection_type_impl& ctype,
        const tombstone& tomb,
        boost::iterator_range<Iterator> cells) {
    auto element_size = [] (size_t c, auto&& e) -> size_t {
        return c + 8 + e.first.size() + e.second.serialize().size();
    };
    auto size = accumulate(cells, (size_t)4, element_size);
    size += 1;
    if (tomb) {
        size += sizeof(tomb.timestamp) + sizeof(tomb.deletion_time);
    }
    bytes ret(bytes::initialized_later(), size);
    bytes::iterator out = ret.begin();
    *out++ = bool(tomb);
    if (tomb) {
        write(out, tomb.timestamp);
        write(out, tomb.deletion_time.time_since_epoch().count());
    }
    auto writeb = [&out] (bytes_view v) {
        serialize_int32(out, v.size());
        out = std::copy_n(v.begin(), v.size(), out);
    };
    // FIXME: overflow?
    serialize_int32(out, boost::distance(cells));
    for (auto&& kv : cells) {
        auto&& k = kv.first;
        auto&& v = kv.second;
        writeb(k);

        writeb(v.serialize());
    }
    return collection_mutation(ctype, ret);
}

bool collection_type_impl::mutation::compact_and_expire(column_id id, row_tombstone base_tomb, gc_clock::time_point query_time,
    can_gc_fn& can_gc, gc_clock::time_point gc_before, compaction_garbage_collector* collector)
{
    bool any_live = false;
    auto t = tomb;
    tombstone purged_tomb;
    if (tomb <= base_tomb.regular()) {
        tomb = tombstone();
    } else if (tomb.deletion_time < gc_before && can_gc(tomb)) {
        purged_tomb = tomb;
        tomb = tombstone();
    }
    t.apply(base_tomb.regular());
    utils::chunked_vector<std::pair<bytes, atomic_cell>> survivors;
    utils::chunked_vector<std::pair<bytes, atomic_cell>> losers;
    for (auto&& name_and_cell : cells) {
        atomic_cell& cell = name_and_cell.second;
        auto cannot_erase_cell = [&] {
            return cell.deletion_time() >= gc_before || !can_gc(tombstone(cell.timestamp(), cell.deletion_time()));
        };

        if (cell.is_covered_by(t, false) || cell.is_covered_by(base_tomb.shadowable().tomb(), false)) {
            continue;
        }
        if (cell.has_expired(query_time)) {
            if (cannot_erase_cell()) {
                survivors.emplace_back(std::make_pair(
                    std::move(name_and_cell.first), atomic_cell::make_dead(cell.timestamp(), cell.deletion_time())));
            } else if (collector) {
                losers.emplace_back(std::pair(
                        std::move(name_and_cell.first), atomic_cell::make_dead(cell.timestamp(), cell.deletion_time())));
            }
        } else if (!cell.is_live()) {
            if (cannot_erase_cell()) {
                survivors.emplace_back(std::move(name_and_cell));
            } else if (collector) {
                losers.emplace_back(std::move(name_and_cell));
            }
        } else {
            any_live |= true;
            survivors.emplace_back(std::move(name_and_cell));
        }
    }
    if (collector) {
        collector->collect(id, mutation{purged_tomb, std::move(losers)});
    }
    cells = std::move(survivors);
    return any_live;
}

collection_mutation
collection_type_impl::serialize_mutation_form(const mutation& mut) const {
    return do_serialize_mutation_form(*this, mut.tomb, boost::make_iterator_range(mut.cells.begin(), mut.cells.end()));
}

collection_mutation
collection_type_impl::serialize_mutation_form(mutation_view mut) const {
    return do_serialize_mutation_form(*this, mut.tomb, boost::make_iterator_range(mut.cells.begin(), mut.cells.end()));
}

collection_mutation
collection_type_impl::serialize_mutation_form_only_live(mutation_view mut, gc_clock::time_point now) const {
    return do_serialize_mutation_form(*this, mut.tomb, mut.cells | boost::adaptors::filtered([t = mut.tomb, now] (auto&& e) {
        return e.second.is_live(t, now, false);
    }));
}

collection_mutation
collection_type_impl::merge(collection_mutation_view a, collection_mutation_view b) const {
 return a.data.with_linearized([&] (bytes_view a_in) {
  return b.data.with_linearized([&] (bytes_view b_in) {
    auto aa = deserialize_mutation_form(a_in);
    auto bb = deserialize_mutation_form(b_in);
    mutation_view merged;
    merged.cells.reserve(aa.cells.size() + bb.cells.size());
    using element_type = std::pair<bytes_view, atomic_cell_view>;
    auto key_type = name_comparator();
    auto compare = [key_type] (const element_type& e1, const element_type& e2) {
        return key_type->less(e1.first, e2.first);
    };
    auto merge = [this] (const element_type& e1, const element_type& e2) {
        // FIXME: use std::max()?
        return std::make_pair(e1.first, compare_atomic_cell_for_merge(e1.second, e2.second) > 0 ? e1.second : e2.second);
    };
    // applied to a tombstone, returns a predicate checking whether a cell is killed by
    // the tombstone
    auto cell_killed = [] (const std::optional<tombstone>& t) {
        return [&t] (const element_type& e) {
            if (!t) {
                return false;
            }
            // tombstone wins if timestamps equal here, unlike row tombstones
            if (t->timestamp < e.second.timestamp()) {
                return false;
            }
            return true;
            // FIXME: should we consider TTLs too?
        };
    };
    combine(aa.cells.begin(), std::remove_if(aa.cells.begin(), aa.cells.end(), cell_killed(bb.tomb)),
            bb.cells.begin(), std::remove_if(bb.cells.begin(), bb.cells.end(), cell_killed(aa.tomb)),
            std::back_inserter(merged.cells),
            compare,
            merge);
    merged.tomb = std::max(aa.tomb, bb.tomb);
    return serialize_mutation_form(std::move(merged));
  });
 });
}

collection_mutation
collection_type_impl::difference(collection_mutation_view a, collection_mutation_view b) const
{
 return a.data.with_linearized([&] (bytes_view a_in) {
  return b.data.with_linearized([&] (bytes_view b_in) {
    auto aa = deserialize_mutation_form(a_in);
    auto bb = deserialize_mutation_form(b_in);
    mutation_view diff;
    diff.cells.reserve(std::max(aa.cells.size(), bb.cells.size()));
    auto key_type = name_comparator();
    auto it = bb.cells.begin();
    for (auto&& c : aa.cells) {
        while (it != bb.cells.end() && key_type->less(it->first, c.first)) {
            ++it;
        }
        if (it == bb.cells.end() || !key_type->equal(it->first, c.first)
            || compare_atomic_cell_for_merge(c.second, it->second) > 0) {

            auto cell = std::make_pair(c.first, c.second);
            diff.cells.emplace_back(std::move(cell));
        }
    }
    if (aa.tomb > bb.tomb) {
        diff.tomb = aa.tomb;
    }
    return serialize_mutation_form(std::move(diff));
  });
 });
}

bytes_opt
collection_type_impl::reserialize(cql_serialization_format from, cql_serialization_format to, bytes_view_opt v) const {
    if (!v) {
        return std::nullopt;
    }
    auto val = deserialize(*v, from);
    bytes ret(bytes::initialized_later(), val.serialized_size());  // FIXME: serialized_size want @to
    auto out = ret.begin();
    serialize(get_value_ptr(val), out, to);
    return ret;
}

set_type
set_type_impl::get_instance(data_type elements, bool is_multi_cell) {
    return intern::get_instance(elements, is_multi_cell);
}

sstring make_set_type_name(data_type elements, bool is_multi_cell)
{
    sstring ret = "";
    if (!is_multi_cell) {
        ret = "org.apache.cassandra.db.marshal.FrozenType(";
    }
    ret += "org.apache.cassandra.db.marshal.SetType(" + elements->name() + ")";
    if (!is_multi_cell) {
        ret += ")";
    }
    return ret;
}

set_type_impl::set_type_impl(data_type elements, bool is_multi_cell)
    : concrete_type(kind::set, make_set_type_name(elements, is_multi_cell), elements, is_multi_cell) {}

data_type
set_type_impl::value_comparator() const {
    return empty_type;
}

data_type
set_type_impl::freeze() const {
    if (_is_multi_cell) {
        return get_instance(_elements, false);
    } else {
        return shared_from_this();
    }
}

bool
set_type_impl::is_compatible_with_frozen(const collection_type_impl& previous) const {
    assert(!_is_multi_cell);
    auto* p = dynamic_cast<const set_type_impl*>(&previous);
    if (!p) {
        return false;
    }
    return _elements->is_compatible_with(*p->_elements);

}

bool
set_type_impl::is_value_compatible_with_frozen(const collection_type_impl& previous) const {
    return is_compatible_with(previous);
}

static void validate_aux(const set_type_impl& t, bytes_view v, cql_serialization_format sf) {
    auto nr = read_collection_size(v, sf);
    for (int i = 0; i != nr; ++i) {
        t.get_elements_type()->validate(read_collection_value(v, sf), sf);
    }
}

static size_t listlike_serialized_size(const std::vector<data_value>* s) {
    size_t len = collection_size_len(cql_serialization_format::internal());
    size_t psz = collection_value_len(cql_serialization_format::internal());
    for (auto&& e : *s) {
        len += psz + e.serialized_size();
    }
    return len;
}

void
set_type_impl::serialize(const void* value, bytes::iterator& out, cql_serialization_format sf) const {
    auto& s = from_value(value);
    write_collection_size(out, s.size(), sf);
    for (auto&& e : s) {
        write_collection_value(out, sf, _elements, e);
    }
}

data_value
set_type_impl::deserialize(bytes_view in, cql_serialization_format sf) const {
    auto nr = read_collection_size(in, sf);
    native_type s;
    s.reserve(nr);
    for (int i = 0; i != nr; ++i) {
        auto e = _elements->deserialize(read_collection_value(in, sf));
        if (e.is_null()) {
            throw marshal_exception("Cannot deserialize a set");
        }
        s.push_back(std::move(e));
    }
    return make_value(std::move(s));
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
        out << t.get_elements_type()->to_json_string(e);
    });
    out << ']';
    return out.str();
}

static bytes from_json_object_aux(const set_type_impl& t, const Json::Value& value, cql_serialization_format sf) {
    if (!value.isArray()) {
        throw marshal_exception("set_type must be represented as JSON Array");
    }
    std::vector<bytes> raw_set;
    raw_set.reserve(value.size());
    for (const Json::Value& v : value) {
        raw_set.emplace_back(t.get_elements_type()->from_json_object(v, sf));
    }
    return collection_type_impl::pack(raw_set.begin(), raw_set.end(), raw_set.size(), sf);
}

std::vector<bytes>
set_type_impl::serialized_values(std::vector<atomic_cell> cells) const {
    // FIXME:
    abort();
}

bytes
set_type_impl::to_value(mutation_view mut, cql_serialization_format sf) const {
    std::vector<bytes_view> tmp;
    tmp.reserve(mut.cells.size());
    for (auto&& e : mut.cells) {
        if (e.second.is_live(mut.tomb, false)) {
            tmp.emplace_back(e.first);
        }
    }
    return pack(tmp.begin(), tmp.end(), tmp.size(), sf);
}

bytes
set_type_impl::serialize_partially_deserialized_form(
        const std::vector<bytes_view>& v, cql_serialization_format sf) const {
    return pack(v.begin(), v.end(), v.size(), sf);
}

list_type
list_type_impl::get_instance(data_type elements, bool is_multi_cell) {
    return intern::get_instance(elements, is_multi_cell);
}

sstring make_list_type_name(data_type elements, bool is_multi_cell)
{
    sstring ret = "";
    if (!is_multi_cell) {
        ret = "org.apache.cassandra.db.marshal.FrozenType(";
    }
    ret += "org.apache.cassandra.db.marshal.ListType(" + elements->name() + ")";
    if (!is_multi_cell) {
        ret += ")";
    }
    return ret;
}

list_type_impl::list_type_impl(data_type elements, bool is_multi_cell)
    : concrete_type(kind::list, make_list_type_name(elements, is_multi_cell), elements, is_multi_cell) {}

data_type
list_type_impl::name_comparator() const {
    return timeuuid_type;
}

data_type
list_type_impl::value_comparator() const {
    return _elements;
}

data_type
list_type_impl::freeze() const {
    if (_is_multi_cell) {
        return get_instance(_elements, false);
    } else {
        return shared_from_this();
    }
}

bool
list_type_impl::is_compatible_with_frozen(const collection_type_impl& previous) const {
    assert(!_is_multi_cell);
    auto* p = dynamic_cast<const list_type_impl*>(&previous);
    if (!p) {
        return false;
    }
    return _elements->is_compatible_with(*p->_elements);

}

static bool is_value_compatible_with_internal(const abstract_type& t, const abstract_type& other);

bool
list_type_impl::is_value_compatible_with_frozen(const collection_type_impl& previous) const {
    auto& lp = dynamic_cast<const list_type_impl&>(previous);
    return is_value_compatible_with_internal(*_elements, *lp._elements);
}

static void validate_aux(const list_type_impl& t, bytes_view v, cql_serialization_format sf) {
    auto nr = read_collection_size(v, sf);
    for (int i = 0; i != nr; ++i) {
        t.get_elements_type()->validate(read_collection_value(v, sf), sf);
    }
    if (!v.empty()) {
        throw marshal_exception(format("Validation failed for type {}: bytes remaining after "
                                       "reading all {} elements of the list -> [{}]",
                t.name(), nr, to_hex(v)));
    }
}

void
list_type_impl::serialize(const void* value, bytes::iterator& out, cql_serialization_format sf) const {
    auto& s = from_value(value);
    write_collection_size(out, s.size(), sf);
    for (auto&& e : s) {
        write_collection_value(out, sf, _elements, e);
    }
}

data_value
list_type_impl::deserialize(bytes_view in, cql_serialization_format sf) const {
    auto nr = read_collection_size(in, sf);
    native_type s;
    s.reserve(nr);
    for (int i = 0; i != nr; ++i) {
        auto e = _elements->deserialize(read_collection_value(in, sf));
        if (e.is_null()) {
            throw marshal_exception("Cannot deserialize a list");
        }
        s.push_back(std::move(e));
    }
    return make_value(std::move(s));
}

static sstring vector_to_string(const std::vector<data_value>& v, std::string_view sep) {
    return join(sstring(sep),
            v | boost::adaptors::transformed([] (const data_value& e) { return e.type()->to_string_impl(e); }));
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
        out << t.get_elements_type()->to_json_string(e);
    });
    out << ']';
    return out.str();
}

static bytes from_json_object_aux(const list_type_impl& t, const Json::Value& value, cql_serialization_format sf) {
    std::vector<bytes> values;
    if (!value.isArray()) {
        throw marshal_exception("list_type must be represented as JSON Array");
    }
    for (const Json::Value& v : value) {
        values.emplace_back(t.get_elements_type()->from_json_object(v, sf));
    }
    return collection_type_impl::pack(values.begin(), values.end(), values.size(), sf);
}

std::vector<bytes>
list_type_impl::serialized_values(std::vector<atomic_cell> cells) const {
    // FIXME:
    abort();
}

bytes
list_type_impl::to_value(mutation_view mut, cql_serialization_format sf) const {
    std::vector<bytes> linearized;
    std::vector<bytes_view> tmp;
    tmp.reserve(mut.cells.size());
    for (auto&& e : mut.cells) {
        if (e.second.is_live(mut.tomb, false)) {
            auto value_view = e.second.value();
            if (value_view.is_fragmented()) {
                auto& v = linearized.emplace_back(value_view.linearize());
                tmp.emplace_back(v);
            } else {
                tmp.emplace_back(value_view.first_fragment());
            }
        }
    }
    return pack(tmp.begin(), tmp.end(), tmp.size(), sf);
}

template <typename F>
static std::optional<data_type> update_listlike(
        const listlike_collection_type_impl& c, F&& f, shared_ptr<const user_type_impl> updated) {
    if (auto e = c.get_elements_type()->update_user_type(updated)) {
        return std::make_optional<data_type>(f(std::move(*e), c.is_multi_cell()));
    }
    return std::nullopt;
}

tuple_type_impl::tuple_type_impl(kind k, sstring name, std::vector<data_type> types)
        : concrete_type(k, std::move(name), { }, data::type_info::make_variable_size()), _types(std::move(types)) {
    for (auto& t : _types) {
        t = t->freeze();
    }
}

tuple_type_impl::tuple_type_impl(std::vector<data_type> types)
        : concrete_type(kind::tuple, make_name(types), { }, data::type_info::make_variable_size()), _types(std::move(types)) {
    for (auto& t : _types) {
        t = t->freeze();
    }
}

shared_ptr<const tuple_type_impl>
tuple_type_impl::get_instance(std::vector<data_type> types) {
    return intern::get_instance(std::move(types));
}

static void validate_aux(const tuple_type_impl& t, bytes_view v, cql_serialization_format sf) {
    auto ti = t.all_types().begin();
    auto vi = tuple_deserializing_iterator::start(v);
    auto end = tuple_deserializing_iterator::finish(v);
    while (ti != t.all_types().end() && vi != end) {
        if (*vi) {
            (*ti)->validate(**vi, sf);
        }
        ++ti;
        ++vi;
    }
}

namespace {
struct validate_visitor {
    bytes_view v;
    cql_serialization_format sf;
    void operator()(const reversed_type_impl& t) { return t.underlying_type()->validate(v, sf); }
    void operator()(const abstract_type&) {}
    template <typename T> void operator()(const integer_type_impl<T>& t) {
        if (v.size() != 0 && v.size() != sizeof(T)) {
            throw marshal_exception(format("Validation failed for type {}: got {:d} bytes", t.name(), v.size()));
        }
    }
    void operator()(const byte_type_impl& t) {
        if (v.size() != 0 && v.size() != 1) {
            throw marshal_exception(format("Expected 1 byte for a tinyint ({:d})", v.size()));
        }
    }
    void operator()(const short_type_impl& t) {
        if (v.size() != 0 && v.size() != 2) {
            throw marshal_exception(format("Expected 2 bytes for a smallint ({:d})", v.size()));
        }
    }
    void operator()(const string_type_impl& t) {
        if (t.as_cql3_type() == cql3::cql3_type::ascii) {
            if (!utils::ascii::validate(v)) {
                throw marshal_exception("Validation failed - non-ASCII character in an ASCII string");
            }
        } else {
            if (!utils::utf8::validate(v)) {
                throw marshal_exception("Validation failed - non-UTF8 character in a UTF8 string");
            }
        }
    }
    void operator()(const bytes_type_impl& t) {}
    void operator()(const boolean_type_impl& t) {
        if (v.size() != 0 && v.size() != 1) {
            throw marshal_exception(format("Validation failed for boolean, got {:d} bytes", v.size()));
        }
    }
    void operator()(const timeuuid_type_impl& t) {
        if (v.size() != 0 && v.size() != 16) {
            throw marshal_exception(format("Validation failed for timeuuid - got {:d} bytes", v.size()));
        }
        auto msb = read_simple<uint64_t>(v);
        auto lsb = read_simple<uint64_t>(v);
        utils::UUID uuid(msb, lsb);
        if (uuid.version() != 1) {
            throw marshal_exception(format("Unsupported UUID version ({:d})", uuid.version()));
        }
    }
    void operator()(const timestamp_type_impl& t) {
        if (v.size() != 0 && v.size() != sizeof(uint64_t)) {
            throw marshal_exception(format("Validation failed for timestamp - got {:d} bytes", v.size()));
        }
    }
    void operator()(const duration_type_impl& t) {
        if (v.size() < 3) {
            throw marshal_exception(format("Expected at least 3 bytes for a duration, got {:d}", v.size()));
        }

        common_counter_type months, days, nanoseconds;
        std::tie(months, days, nanoseconds) = deserialize_counters(v);

        auto check_counter_range = [] (common_counter_type value, auto counter_value_type_instance,
                                           std::string_view counter_name) {
            using counter_value_type = decltype(counter_value_type_instance);

            if (static_cast<counter_value_type>(value) != value) {
                throw marshal_exception(format("The duration {} ({:d}) must be a {:d} bit integer", counter_name, value,
                        std::numeric_limits<counter_value_type>::digits + 1));
            }
        };

        check_counter_range(months, months_counter::value_type(), "months");
        check_counter_range(days, days_counter::value_type(), "days");
        check_counter_range(nanoseconds, nanoseconds_counter::value_type(), "nanoseconds");

        if (!(((months <= 0) && (days <= 0) && (nanoseconds <= 0)) ||
                    ((months >= 0) && (days >= 0) && (nanoseconds >= 0)))) {
            throw marshal_exception(format("The duration months, days, and nanoseconds must be all of "
                                           "the same sign ({:d}, {:d}, {:d})",
                    months, days, nanoseconds));
        }
    }
    template <typename T> void operator()(const floating_type_impl<T>& t) {
        if (v.size() != 0 && v.size() != sizeof(T)) {
            throw marshal_exception(format("Expected {:d} bytes for a floating type, got {:d}", sizeof(T), v.size()));
        }
    }
    void operator()(const simple_date_type_impl& t) {
        if (v.size() != 0 && v.size() != 4) {
            throw marshal_exception(format("Expected 4 byte long for date ({:d})", v.size()));
        }
    }
    void operator()(const time_type_impl& t) {
        if (v.size() != 0 && v.size() != 8) {
            throw marshal_exception(format("Expected 8 byte long for time ({:d})", v.size()));
        }
    }
    void operator()(const uuid_type_impl& t) {
        if (v.size() != 0 && v.size() != 16) {
            throw marshal_exception(format("Validation failed for uuid - got {:d} bytes", v.size()));
        }
    }
    void operator()(const inet_addr_type_impl& t) {
        if (v.size() != 0 && v.size() != sizeof(uint32_t) && v.size() != 16) {
            throw marshal_exception(format("Validation failed for inet_addr - got {:d} bytes", v.size()));
        }
    }
    void operator()(const map_type_impl& t) { validate_aux(t, v, sf); }
    void operator()(const set_type_impl& t) { validate_aux(t, v, sf); }
    void operator()(const list_type_impl& t) { validate_aux(t, v, sf); }
    void operator()(const tuple_type_impl& t) { validate_aux(t, v, sf); }
};
}

void abstract_type::validate(bytes_view v, cql_serialization_format sf) const {
    visit(*this, validate_visitor{v, sf});
}

static void serialize_aux(const tuple_type_impl& t, const tuple_type_impl::native_type* v, bytes::iterator& out) {
    auto do_serialize = [&out] (auto&& t_v) {
        const data_type& t = boost::get<0>(t_v);
        const data_value& v = boost::get<1>(t_v);
        if (!v.is_null() && t != v.type()) {
            throw std::runtime_error("tuple element type mismatch");
        }
        if (v.is_null()) {
            write(out, int32_t(-1));
        } else {
            write(out, int32_t(v.serialized_size()));
            v.serialize(out);
        }
    };
    boost::range::for_each(boost::combine(t.all_types(), *v), do_serialize);
}

static size_t concrete_serialized_size(const varint_type_impl::native_type& v);

static void serialize(const abstract_type& t, const void* value, bytes::iterator& out);

namespace {
struct serialize_visitor {
    bytes::iterator& out;
    void operator()(const reversed_type_impl& t, const void* v) { return serialize(*t.underlying_type(), v, out); }
    template <typename T>
    void operator()(const integer_type_impl<T>& t, const typename integer_type_impl<T>::native_type* v1) {
        if (v1->empty()) {
            return;
        }
        auto v = v1->get();
        auto u = net::hton(v);
        out = std::copy_n(reinterpret_cast<const char*>(&u), sizeof(u), out);
    }
    void operator()(const string_type_impl& t, const string_type_impl::native_type* v) {
        out = std::copy(v->begin(), v->end(), out);
    }
    void operator()(const bytes_type_impl& t, const bytes* v) {
        out = std::copy(v->begin(), v->end(), out);
    }
    void operator()(const boolean_type_impl& t, const boolean_type_impl::native_type* v) {
        if (!v->empty()) {
            *out++ = char(*v);
        }
    }
    void operator()(const date_type_impl& t, const date_type_impl::native_type* v) {
        if (v->empty()) {
            return;
        }
        int64_t i = v->get().time_since_epoch().count();
        i = net::hton(uint64_t(i));
        out = std::copy_n(reinterpret_cast<const char*>(&i), sizeof(i), out);
    }
    void operator()(const timeuuid_type_impl& t, const timeuuid_type_impl::native_type* uuid1) {
        if (uuid1->empty()) {
            return;
        }
        auto uuid = uuid1->get();
        uuid.serialize(out);
    }
    void operator()(const timestamp_type_impl& t, const timestamp_type_impl::native_type* v1) {
        if (v1->empty()) {
            return;
        }
        uint64_t v = v1->get().time_since_epoch().count();
        v = net::hton(v);
        out = std::copy_n(reinterpret_cast<const char*>(&v), sizeof(v), out);
    }
    void operator()(const simple_date_type_impl& t, const simple_date_type_impl::native_type* v1) {
        if (v1->empty()) {
            return;
        }
        uint32_t v = v1->get();
        v = net::hton(v);
        out = std::copy_n(reinterpret_cast<const char*>(&v), sizeof(v), out);
    }
    void operator()(const time_type_impl& t, const time_type_impl::native_type* v1) {
        if (v1->empty()) {
            return;
        }
        uint64_t v = v1->get();
        v = net::hton(v);
        out = std::copy_n(reinterpret_cast<const char*>(&v), sizeof(v), out);
    }
    void operator()(const empty_type_impl& t, const void*) {}
    void operator()(const uuid_type_impl& t, const uuid_type_impl::native_type* value) {
        value->get().serialize(out);
    }
    void operator()(const inet_addr_type_impl& t, const inet_addr_type_impl::native_type* ipv) {
        if (ipv->empty()) {
            return;
        }
        auto& ip = ipv->get();
        switch (ip.in_family()) {
        case inet_address::family::INET: {
            const ::in_addr& in = ip;
            out = std::copy_n(reinterpret_cast<const char*>(&in.s_addr), sizeof(in.s_addr), out);
            break;
        }
        case inet_address::family::INET6: {
            const ::in6_addr& i6 = ip;
            out = std::copy_n(i6.s6_addr, ip.size(), out);
            break;
        }
        }
    }
    template <typename T>
    void operator()(const floating_type_impl<T>& t, const typename floating_type_impl<T>::native_type* value) {
        T d = *value;
        if (std::isnan(d)) {
            // Java's Double.doubleToLongBits() documentation specifies that
            // any nan must be serialized to the same specific value
            d = std::numeric_limits<T>::quiet_NaN();
        }
        typename int_of_size<T>::itype i;
        memcpy(&i, &d, sizeof(T));
        auto u = net::hton(i);
        out = std::copy_n(reinterpret_cast<const char*>(&u), sizeof(u), out);
    }
    void operator()(const varint_type_impl& t, const varint_type_impl::native_type* num1) {
        if (num1->empty()) {
            return;
        }
        auto& num = std::move(*num1).get();
        boost::multiprecision::cpp_int pnum = boost::multiprecision::abs(num);

        std::vector<uint8_t> b;
        auto size = concrete_serialized_size(*num1);
        b.reserve(size);
        if (num.sign() < 0) {
            pnum -= 1;
        }
        for (unsigned i = 0; i < size; i++) {
            auto v = boost::multiprecision::integer_modulus(pnum, 256);
            pnum >>= 8;
            if (num.sign() < 0) {
                v = ~v;
            }
            b.push_back(v);
        }
        out = std::copy(b.crbegin(), b.crend(), out);
    }
    void operator()(const decimal_type_impl& t, const decimal_type_impl::native_type* bd1) {
        if (bd1->empty()) {
            return;
        }
        auto&& bd = std::move(*bd1).get();
        auto u = net::hton(bd.scale());
        out = std::copy_n(reinterpret_cast<const char*>(&u), sizeof(int32_t), out);
        varint_type_impl::native_type unscaled_value = bd.unscaled_value();
        const auto* real_varint_type = reinterpret_cast<const varint_type_impl*>(varint_type.get());
        serialize_visitor{out}(*real_varint_type, &unscaled_value);
    }
    void operator()(const counter_type_impl& t, const void*) { fail(unimplemented::cause::COUNTERS); }
    void operator()(const duration_type_impl& t, const duration_type_impl::native_type* m) {
        if (m->empty()) {
            return;
        }
        const auto& d = m->get();
        out += signed_vint::serialize(d.months, out);
        out += signed_vint::serialize(d.days, out);
        out += signed_vint::serialize(d.nanoseconds, out);
    }
    void operator()(const collection_type_impl& t, const void* value) {
        return t.serialize(value, out, cql_serialization_format::internal());
    }
    void operator()(const tuple_type_impl& t, const tuple_type_impl::native_type* value) {
        return serialize_aux(t, value, out);
    }
};
}

static void serialize(const abstract_type& t, const void* value, bytes::iterator& out)  {
    visit(t, value, serialize_visitor{out});
}

static data_value deserialize_aux(const tuple_type_impl& t, bytes_view v) {
    tuple_type_impl::native_type ret;
    ret.reserve(t.all_types().size());
    auto ti = t.all_types().begin();
    auto vi = tuple_deserializing_iterator::start(v);
    while (ti != t.all_types().end() && vi != tuple_deserializing_iterator::finish(v)) {
        data_value obj = data_value::make_null(*ti);
        if (*vi) {
            obj = (*ti)->deserialize(**vi);
        }
        ret.push_back(std::move(obj));
        ++ti;
        ++vi;
    }
    while (ti != t.all_types().end()) {
        ret.push_back(data_value::make_null(*ti++));
    }
    return data_value::make(t.shared_from_this(), std::make_unique<tuple_type_impl::native_type>(std::move(ret)));
}

static boost::multiprecision::cpp_int deserialize_value(const varint_type_impl&, bytes_view v) {
    auto negative = v.front() < 0;
    boost::multiprecision::cpp_int num;
    for (uint8_t b : v) {
        if (negative) {
            b = ~b;
        }
        num <<= 8;
        num += b;
    }
    if (negative) {
        num += 1;
    }
    return negative ? -num : num;
}

template <typename T> static T deserialize_value(const floating_type_impl<T>&, bytes_view v) {
    typename int_of_size<T>::itype i = read_simple<typename int_of_size<T>::itype>(v);
    if (!v.empty()) {
        throw marshal_exception(format("cannot deserialize floating - {:d} bytes left", v.size()));
    }
    T d;
    memcpy(&d, &i, sizeof(T));
    return d;
}

static big_decimal deserialize_value(const decimal_type_impl& , bytes_view v) {
    auto scale = read_simple<int32_t>(v);
    auto unscaled = deserialize_value(static_cast<const varint_type_impl&>(*varint_type), v);
    return big_decimal(scale, unscaled);
}

static cql_duration deserialize_value(const duration_type_impl& t, bytes_view v) {
    common_counter_type months, days, nanoseconds;
    std::tie(months, days, nanoseconds) = deserialize_counters(v);
    return cql_duration(months_counter(months), days_counter(days), nanoseconds_counter(nanoseconds));
}

static inet_address deserialize_value(const inet_addr_type_impl&, bytes_view v) {
    switch (v.size()) {
    case 4:
        // gah. read_simple_be, please...
        return inet_address(::in_addr{net::hton(read_simple<uint32_t>(v))});
    case 16:
        return inet_address(*reinterpret_cast<const ::in6_addr*>(v.data()));
    default:
        throw marshal_exception(format("cannot deserialize inet_address, unsupported size {:d} bytes", v.size()));
    }
}

static utils::UUID deserialize_value(const uuid_type_impl&, bytes_view v) {
    auto msb = read_simple<uint64_t>(v);
    auto lsb = read_simple<uint64_t>(v);
    if (!v.empty()) {
        throw marshal_exception(format("cannot deserialize uuid, {:d} bytes left", v.size()));
    }
    return utils::UUID(msb, lsb);
}

static utils::UUID deserialize_value(const timeuuid_type_impl&, bytes_view v) {
    return deserialize_value(static_cast<const uuid_type_impl&>(*uuid_type), v);
}

static db_clock::time_point deserialize_value(const timestamp_type_impl&, bytes_view v) {
    auto v2 = read_simple_exactly<uint64_t>(v);
    return db_clock::time_point(db_clock::duration(v2));
}

static uint32_t deserialize_value(const simple_date_type_impl&, bytes_view v) {
    return read_simple_exactly<uint32_t>(v);
}

static int64_t deserialize_value(const time_type_impl&, bytes_view v) {
    return read_simple_exactly<int64_t>(v);
}

static db_clock::time_point deserialize_value(const date_type_impl&, bytes_view v) {
    auto tmp = read_simple_exactly<uint64_t>(v);
    return db_clock::time_point(db_clock::duration(tmp));
}

static bool deserialize_value(const boolean_type_impl&, bytes_view v) {
    if (v.size() != 1) {
        throw marshal_exception(format("cannot deserialize boolean, size mismatch ({:d})", v.size()));
    }
    return *v.begin() != 0;
}

template<typename T>
static T deserialize_value(const integer_type_impl<T>& t, bytes_view v) {
    return read_simple_exactly<T>(v);
}

static sstring deserialize_value(const string_type_impl&, bytes_view v) {
    // FIXME: validation?
    return sstring(reinterpret_cast<const char*>(v.begin()), v.size());
}

namespace {
struct deserialize_visitor {
    bytes_view v;
    data_value operator()(const reversed_type_impl& t) { return t.underlying_type()->deserialize(v); }
    template <typename T> data_value operator()(const T& t) {
        if (v.empty()) {
            return t.make_empty();
        }
        return t.make_value(deserialize_value(t, v));
    }
    data_value operator()(const ascii_type_impl& t) {
         return t.make_value(deserialize_value(t, v));
    }
    data_value operator()(const utf8_type_impl& t) {
         return t.make_value(deserialize_value(t, v));
    }
    data_value operator()(const bytes_type_impl& t) {
        return t.make_value(std::make_unique<bytes_type_impl::native_type>(v.begin(), v.end()));
    }
    data_value operator()(const counter_type_impl& t) { fail(unimplemented::cause::COUNTERS); }
    data_value operator()(const list_type_impl& t) {
        return t.deserialize(v, cql_serialization_format::internal());
    }
    data_value operator()(const map_type_impl& t) {
        return t.deserialize(v, cql_serialization_format::internal());
    }
    data_value operator()(const set_type_impl& t) {
        return t.deserialize(v, cql_serialization_format::internal());
    }
    data_value operator()(const tuple_type_impl& t) { return deserialize_aux(t, v); }
    data_value operator()(const user_type_impl& t) { return deserialize_aux(t, v); }
    data_value operator()(const empty_type_impl& t) { return data_value::make_null(t.shared_from_this()); }
};
}

data_value abstract_type::deserialize(bytes_view v) const {
    return visit(*this, deserialize_visitor{v});
}

namespace {
struct compare_visitor {
    bytes_view v1;
    bytes_view v2;
    template <typename T> int32_t operator()(const simple_type_impl<T>&) {
        if (v1.empty()) {
            return v2.empty() ? 0 : -1;
        }
        if (v2.empty()) {
            return 1;
        }
        T a = simple_type_traits<T>::read_nonempty(v1);
        T b = simple_type_traits<T>::read_nonempty(v2);
        return a == b ? 0 : a < b ? -1 : 1;
    }
    int32_t operator()(const string_type_impl&) { return compare_unsigned(v1, v2); }
    int32_t operator()(const bytes_type_impl&) { return compare_unsigned(v1, v2); }
    int32_t operator()(const duration_type_impl&) { return compare_unsigned(v1, v2); }
    int32_t operator()(const inet_addr_type_impl&) { return compare_unsigned(v1, v2); }
    int32_t operator()(const date_type_impl&) { return compare_unsigned(v1, v2); }
    int32_t operator()(const timeuuid_type_impl&) {
        if (v1.empty()) {
            return v2.empty() ? 0 : -1;
        }
        if (v2.empty()) {
            return 1;
        }
        if (auto r = timeuuid_compare_bytes(v1, v2)) {
            return r;
        }
        return lexicographical_tri_compare(
                v1.begin(), v1.end(), v2.begin(), v2.end(), [] (const int8_t& a, const int8_t& b) { return a - b; });
    }
    int32_t operator()(const listlike_collection_type_impl& l) {
        using llpdi = listlike_partial_deserializing_iterator;
        auto sf = cql_serialization_format::internal();
        return lexicographical_tri_compare(llpdi::begin(v1, sf), llpdi::end(v1, sf), llpdi::begin(v2, sf),
                llpdi::end(v2, sf),
                [&] (bytes_view o1, bytes_view o2) { return l.get_elements_type()->compare(o1, o2); });
    }
    int32_t operator()(const map_type_impl& m) {
        return map_type_impl::compare_maps(m.get_keys_type(), m.get_values_type(), v1, v2);
    }
    int32_t operator()(const uuid_type_impl&) {
        if (v1.size() < 16) {
            return v2.size() < 16 ? 0 : -1;
        }
        if (v2.size() < 16) {

            return 1;
        }
        auto c1 = (v1[6] >> 4) & 0x0f;
        auto c2 = (v2[6] >> 4) & 0x0f;

        if (c1 != c2) {
            return c1 - c2;
        }

        if (c1 == 1) {
            if (auto c = timeuuid_compare_bytes(v1, v2)) {
                return c;
            }
        }
        return compare_unsigned(v1, v2);
    }
    int32_t operator()(const empty_type_impl&) { return 0; }
    int32_t operator()(const tuple_type_impl& t) {
        return lexicographical_tri_compare(t.all_types().begin(), t.all_types().end(),
                tuple_deserializing_iterator::start(v1), tuple_deserializing_iterator::finish(v1),
                tuple_deserializing_iterator::start(v2), tuple_deserializing_iterator::finish(v2), tri_compare_opt);
    }
    int32_t operator()(const counter_type_impl&) {
        fail(unimplemented::cause::COUNTERS);
        return 0;
    }
    int32_t operator()(const decimal_type_impl& d) {
        if (v1.empty()) {
            return v2.empty() ? 0 : -1;
        }
        if (v2.empty()) {
            return 1;
        }
        auto a = deserialize_value(d, v1);
        auto b = deserialize_value(d, v2);
        return a.compare(b);
    }
    int32_t operator()(const varint_type_impl& v) {
        if (v1.empty()) {
            return v2.empty() ? 0 : -1;
        }
        if (v2.empty()) {
            return 1;
        }
        auto a = deserialize_value(v, v1);
        auto b = deserialize_value(v, v2);
        return a == b ? 0 : a < b ? -1 : 1;
    }
    template <typename T> int32_t operator()(const floating_type_impl<T>&) {
        if (v1.empty()) {
            return v2.empty() ? 0 : -1;
        }
        if (v2.empty()) {
            return 1;
        }
        T a = simple_type_traits<T>::read_nonempty(v1);
        T b = simple_type_traits<T>::read_nonempty(v2);

        // in java world NaN == NaN and NaN is greater than anything else
        if (std::isnan(a) && std::isnan(b)) {
            return 0;
        } else if (std::isnan(a)) {
            return 1;
        } else if (std::isnan(b)) {
            return -1;
        }
        // also -0 < 0
        if (std::signbit(a) && !std::signbit(b)) {
            return -1;
        } else if (!std::signbit(a) && std::signbit(b)) {
            return 1;
        }
        return a == b ? 0 : a < b ? -1 : 1;
    }
    int32_t operator()(const reversed_type_impl& r) { return r.underlying_type()->compare(v2, v1); }
};
}

int32_t abstract_type::compare(bytes_view v1, bytes_view v2) const {
    return visit(*this, compare_visitor{v1, v2});
}

bool abstract_type::equal(bytes_view v1, bytes_view v2) const {
    return ::visit(*this, [&](const auto& t) {
        if (is_byte_order_equal_visitor{}(t)) {
            return compare_unsigned(v1, v2) == 0;
        }
        return compare_visitor{v1, v2}(t) == 0;
    });
}

std::vector<bytes_view_opt>
tuple_type_impl::split(bytes_view v) const {
    return { tuple_deserializing_iterator::start(v), tuple_deserializing_iterator::finish(v) };
}

// Count number of ':' which are not preceded by '\'.
static std::size_t count_segments(sstring_view v) {
    std::size_t segment_count = 1;
    char prev_ch = '.';
    for (char ch : v) {
        if (ch == ':' && prev_ch != '\\') {
            ++segment_count;
        }
        prev_ch = ch;
    }
    return segment_count;
}

// Split on ':', unless it's preceded by '\'.
static std::vector<sstring_view> split_field_strings(sstring_view v) {
    if (v.empty()) {
        return std::vector<sstring_view>();
    }
    std::vector<sstring_view> result;
    result.reserve(count_segments(v));
    std::size_t prev = 0;
    char prev_ch = '.';
    for (std::size_t i = 0; i < v.size(); ++i) {
        if (v[i] == ':' && prev_ch != '\\') {
            result.push_back(v.substr(prev, i - prev));
            prev = i + 1;
        }
        prev_ch = v[i];
    }
    result.push_back(v.substr(prev, v.size() - prev));
    return result;
}

// Replace "\:" with ":" and "\@" with "@".
static std::string unescape(sstring_view s) {
    return std::regex_replace(std::string(s), std::regex("\\\\([@:])"), "$1");
}

// Replace ":" with "\:" and "@" with "\@".
static std::string escape(sstring_view s) {
    return std::regex_replace(std::string(s), std::regex("[@:]"), "\\$0");
}

// Concat list of bytes into a single bytes.
static bytes concat_fields(const std::vector<bytes>& fields, const std::vector<int32_t> field_len) {
    std::size_t result_size = 4 * fields.size();
    for (int32_t len : field_len) {
        result_size += len > 0 ? len : 0;
    }
    bytes result{bytes::initialized_later(), result_size};
    bytes::iterator it = result.begin();
    for (std::size_t i = 0; i < fields.size(); ++i) {
        int32_t tmp = net::hton(field_len[i]);
        it = std::copy_n(reinterpret_cast<const int8_t*>(&tmp), sizeof(tmp), it);
        if (field_len[i] > 0) {
            it = std::copy(std::begin(fields[i]), std::end(fields[i]), it);
        }
    }
    return result;
}

size_t abstract_type::hash(bytes_view v) const {
    struct visitor {
        bytes_view v;
        size_t operator()(const reversed_type_impl& t) { return t.underlying_type()->hash(v); }
        size_t operator()(const abstract_type& t) { return std::hash<bytes_view>()(v); }
        size_t operator()(const tuple_type_impl& t) {
            auto apply_hash = [] (auto&& type_value) {
                auto&& type = boost::get<0>(type_value);
                auto&& value = boost::get<1>(type_value);
                return value ? type->hash(*value) : 0;
            };
            // FIXME: better accumulation function
            return boost::accumulate(combine(t.all_types(), t.make_range(v)) | boost::adaptors::transformed(apply_hash),
                    0, std::bit_xor<>());
        }
        size_t operator()(const varint_type_impl& t) {
            bytes b(v.begin(), v.end());
            return std::hash<sstring>()(t.to_string(b));
        }
        size_t operator()(const decimal_type_impl& t) {
            bytes b(v.begin(), v.end());
            return std::hash<sstring>()(t.to_string(b));
        }
        size_t operator()(const counter_type_impl&) { fail(unimplemented::cause::COUNTERS); }
        size_t operator()(const empty_type_impl&) { return 0; }
    };
    return visit(*this, visitor{v});
}

static size_t concrete_serialized_size(const byte_type_impl::native_type&) { return sizeof(int8_t); }
static size_t concrete_serialized_size(const short_type_impl::native_type&) { return sizeof(int16_t); }
static size_t concrete_serialized_size(const int32_type_impl::native_type&) { return sizeof(int32_t); }
static size_t concrete_serialized_size(const long_type_impl::native_type&) { return sizeof(int64_t); }
static size_t concrete_serialized_size(const float_type_impl::native_type&) { return sizeof(float); }
static size_t concrete_serialized_size(const double_type_impl::native_type&) { return sizeof(double); }
static size_t concrete_serialized_size(const boolean_type_impl::native_type&) { return 1; }
static size_t concrete_serialized_size(const date_type_impl::native_type&) { return 8; }
static size_t concrete_serialized_size(const timeuuid_type_impl::native_type&) { return 16; }
static size_t concrete_serialized_size(const simple_date_type_impl::native_type&) { return 4; }
static size_t concrete_serialized_size(const string_type_impl::native_type& v) { return v.size(); }
static size_t concrete_serialized_size(const bytes_type_impl::native_type& v) { return v.size(); }
static size_t concrete_serialized_size(const inet_addr_type_impl::native_type& v) { return v.get().size(); }

static size_t concrete_serialized_size(const varint_type_impl::native_type& v) {
    const auto& num = v.get();
    if (!num) {
        return 1;
    }
    auto pnum = abs(num);
    return align_up(boost::multiprecision::msb(pnum) + 2, 8u) / 8;
}

static size_t concrete_serialized_size(const decimal_type_impl::native_type& v) {
    const varint_type_impl::native_type& uv = v.get().unscaled_value();
    return sizeof(int32_t) + concrete_serialized_size(uv);
}

static size_t concrete_serialized_size(const duration_type_impl::native_type& v) {
    const auto& d = v.get();
    return signed_vint::serialized_size(d.months) + signed_vint::serialized_size(d.days) +
           signed_vint::serialized_size(d.nanoseconds);
}

static size_t concrete_serialized_size(const tuple_type_impl::native_type& v) {
    size_t len = 0;
    for (auto&& e : v) {
        len += 4 + e.serialized_size();
    }
    return len;
}

static size_t serialized_size(const abstract_type& t, const void* value);

namespace {
struct serialized_size_visitor {
    size_t operator()(const reversed_type_impl& t, const void* v) { return serialized_size(*t.underlying_type(), v); }
    size_t operator()(const empty_type_impl&, const void*) { return 0; }
    template <typename T>
    size_t operator()(const concrete_type<T>& t, const typename concrete_type<T>::native_type* v) {
        if (v->empty()) {
            return 0;
        }
        return concrete_serialized_size(*v);
    }
    size_t operator()(const counter_type_impl&, const void*) { fail(unimplemented::cause::COUNTERS); }
    size_t operator()(const map_type_impl& t, const map_type_impl::native_type* v) { return map_serialized_size(v); }
    size_t operator()(const concrete_type<std::vector<data_value>, listlike_collection_type_impl>& t,
            const std::vector<data_value>* v) {
        return listlike_serialized_size(v);
    }
};
}

static size_t serialized_size(const abstract_type& t, const void* value) {
    return visit(t, value, serialized_size_visitor{});
}

template<typename T>
static bytes serialize_value(const T& t, const typename T::native_type& v) {
    bytes b(bytes::initialized_later(), serialized_size_visitor{}(t, &v));
    auto i = b.begin();
    serialize_visitor{i}(t, &v);
    return b;
}

namespace {
struct from_string_visitor {
    sstring_view s;
    bytes operator()(const reversed_type_impl& r) { return r.underlying_type()->from_string(s); }
    template <typename T> bytes operator()(const integer_type_impl<T>& t) { return decompose_value(parse_int(t, s)); }
    bytes operator()(const string_type_impl&) {
        return to_bytes(bytes_view(reinterpret_cast<const int8_t*>(s.begin()), s.size()));
    }
    bytes operator()(const bytes_type_impl&) { return from_hex(s); }
    bytes operator()(const boolean_type_impl& t) {
        sstring s_lower(s.begin(), s.end());
        std::transform(s_lower.begin(), s_lower.end(), s_lower.begin(), ::tolower);
        bool v;
        if (s.empty() || s_lower == "false") {
            v = false;
        } else if (s_lower == "true") {
            v = true;
        } else {
            throw marshal_exception(format("unable to make boolean from '{}'", s));
        }
        return serialize_value(t, v);
    }
    bytes operator()(const timeuuid_type_impl&) {
        if (s.empty()) {
            return bytes();
        }
        static const std::regex re("^[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12}$");
        if (!std::regex_match(s.begin(), s.end(), re)) {
            throw marshal_exception(format("Invalid UUID format ({})", s));
        }
        utils::UUID v(s);
        if (v.version() != 1) {
            throw marshal_exception(format("Unsupported UUID version ({:d})", v.version()));
        }
        return v.serialize();
    }
    bytes operator()(const timestamp_type_impl& t) {
        if (s.empty()) {
            return bytes();
        }
        return serialize_value(t, db_clock::time_point(db_clock::duration(timestamp_from_string(s))));
    }
    bytes operator()(const simple_date_type_impl& t) {
        if (s.empty()) {
            return bytes();
        }
        return serialize_value(t, days_from_string(s));
    }
    bytes operator()(const date_type_impl& t) {
        return serialize_value(t, db_clock::time_point(db_clock::duration(timestamp_from_string(s))));
    }
    bytes operator()(const time_type_impl& t) {
        if (s.empty()) {
            return bytes();
        }
        return serialize_value(t, parse_time(s));
    }
    bytes operator()(const uuid_type_impl&) {
        if (s.empty()) {
            return bytes();
        }
        static const std::regex re("^[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12}$");
        if (!std::regex_match(s.begin(), s.end(), re)) {
            throw marshal_exception(format("Cannot parse uuid from '{}'", s));
        }
        utils::UUID v(s);
        return v.serialize();
    }
    template <typename T> bytes operator()(const floating_type_impl<T>& t) {
        if (s.empty()) {
            return bytes();
        }
        try {
            auto d = boost::lexical_cast<T>(s.begin(), s.size());
            return serialize_value(t, d);
        } catch (const boost::bad_lexical_cast& e) {
            throw marshal_exception(format("Invalid number format '{}'", s));
        }
    }
    bytes operator()(const varint_type_impl& t) {
        if (s.empty()) {
            return bytes();
        }
        try {
            std::string str(s.begin(), s.end());
            varint_type_impl::native_type num(str);
            return serialize_value(t, num);
        } catch (...) {
            throw marshal_exception(format("unable to make int from '{}'", s));
        }
    }
    bytes operator()(const decimal_type_impl& t) {
        if (s.empty()) {
            return bytes();
        }
        try {
            decimal_type_impl::native_type bd(s);
            return serialize_value(t, bd);
        } catch (...) {
            throw marshal_exception(format("unable to make BigDecimal from '{}'", s));
        }
    }
    bytes operator()(const counter_type_impl&) {
        fail(unimplemented::cause::COUNTERS);
        return bytes();
    }
    bytes operator()(const duration_type_impl& t) {
        if (s.empty()) {
            return bytes();
        }

        try {
            return serialize_value(t, cql_duration(s));
        } catch (cql_duration_error const& e) {
            throw marshal_exception(e.what());
        }
    }
    bytes operator()(const empty_type_impl&) { return bytes(); }
    bytes operator()(const inet_addr_type_impl& t) {
        // FIXME: support host names
        if (s.empty()) {
            return bytes();
        }
        try {
            auto ip = inet_address(std::string(s.data(), s.size()));
            return serialize_value(t, ip);
        } catch (...) {
            throw marshal_exception(format("Failed to parse inet_addr from '{}'", s));
        }
    }
    bytes operator()(const tuple_type_impl& t) {
        std::vector<sstring_view> field_strings = split_field_strings(s);
        if (field_strings.size() > t.size()) {
            throw marshal_exception(
                    format("Invalid tuple literal: too many elements. Type {} expects {:d} but got {:d}",
                            t.as_cql3_type(), t.size(), field_strings.size()));
        }
        std::vector<bytes> fields(field_strings.size());
        std::vector<int32_t> field_len(field_strings.size(), -1);
        for (std::size_t i = 0; i < field_strings.size(); ++i) {
            if (field_strings[i] != "@") {
                std::string field_string = unescape(field_strings[i]);
                fields[i] = t.type(i)->from_string(field_string);
                field_len[i] = fields[i].size();
            }
        }
        return std::move(concat_fields(fields, field_len));
    }
    bytes operator()(const collection_type_impl&) {
        // FIXME:
        abort();
        return bytes();
    }
};
}

bytes abstract_type::from_string(sstring_view s) const { return visit(*this, from_string_visitor{s}); }

static sstring tuple_to_string(const tuple_type_impl &t, const tuple_type_impl::native_type& b) {
    std::ostringstream out;
    for (size_t i = 0; i < b.size(); ++i) {
        if (i > 0) {
            out << ":";
        }

        const auto& val = b[i];
        if (val.is_null()) {
            out << "@";
        } else {
            // We use ':' as delimiter and '@' to represent null, so they need to be escaped in the tuple's fields.
            auto typ = t.type(i);
            out << escape(typ->to_string(typ->decompose(val)));
        }
    }

    return out.str();
}

template <typename N, typename A, typename F>
static sstring format_if_not_empty(
        const concrete_type<N, A>& type, const typename concrete_type<N, A>::native_type* b, F&& f) {
    if (b->empty()) {
        return {};
    }
    return f(static_cast<const N&>(*b));
}

static sstring to_string_impl(const abstract_type& t, const void* v);

namespace {
struct to_string_impl_visitor {
    template <typename T>
    sstring operator()(const concrete_type<T>& t, const typename concrete_type<T>::native_type* v) {
        return format_if_not_empty(t, v, [] (const T& v) { return to_sstring(v); });
    }
    sstring operator()(const bytes_type_impl& b, const bytes* v) {
        return format_if_not_empty(b, v, [] (const bytes& v) { return to_hex(v); });
    }
    sstring operator()(const boolean_type_impl& b, const boolean_type_impl::native_type* v) {
        return format_if_not_empty(b, v, boolean_to_string);
    }
    sstring operator()(const counter_type_impl& c, const void*) { fail(unimplemented::cause::COUNTERS); }
    sstring operator()(const date_type_impl& d, const date_type_impl::native_type* v) {
        return format_if_not_empty(d, v, [] (const db_clock::time_point& v) { return time_point_to_string(v); });
    }
    sstring operator()(const decimal_type_impl& d, const decimal_type_impl::native_type* v) {
        return format_if_not_empty(d, v, [] (const big_decimal& v) { return v.to_string(); });
    }
    sstring operator()(const duration_type_impl& d, const duration_type_impl::native_type* v) {
        return format_if_not_empty(d, v, [] (const cql_duration& v) { return ::to_string(v); });
    }
    sstring operator()(const empty_type_impl&, const void*) { return sstring(); }
    sstring operator()(const inet_addr_type_impl& a, const inet_addr_type_impl::native_type* v) {
        return format_if_not_empty(a, v, inet_to_string);
    }
    sstring operator()(const list_type_impl& l, const list_type_impl::native_type* v) {
        return format_if_not_empty(
                l, v, [] (const list_type_impl::native_type& v) { return vector_to_string(v, ", "); });
    }
    sstring operator()(const set_type_impl& s, const set_type_impl::native_type* v) {
        return format_if_not_empty(s, v, [] (const set_type_impl::native_type& v) { return vector_to_string(v, "; "); });
    }
    sstring operator()(const map_type_impl& m, const map_type_impl::native_type* v) {
        return format_if_not_empty(
                m, v, [&m] (const map_type_impl::native_type& v) { return map_to_string(v, !m.is_multi_cell()); });
    }
    sstring operator()(const reversed_type_impl& r, const void* v) { return to_string_impl(*r.underlying_type(), v); }
    sstring operator()(const simple_date_type_impl& s, const simple_date_type_impl::native_type* v) {
        return format_if_not_empty(s, v, simple_date_to_string);
    }
    sstring operator()(const string_type_impl& s, const string_type_impl::native_type* v) {
        return format_if_not_empty(s, v, [] (const sstring& s) { return s; });
    }
    sstring operator()(const time_type_impl& t, const time_type_impl::native_type* v) {
        return format_if_not_empty(t, v, time_to_string);
    }
    sstring operator()(const timestamp_type_impl& t, const timestamp_type_impl::native_type* v) {
        return format_if_not_empty(t, v, [] (const db_clock::time_point& v) { return time_point_to_string(v); });
    }
    sstring operator()(const timeuuid_type_impl& t, const timeuuid_type_impl::native_type* v) {
        return format_if_not_empty(t, v, [] (const utils::UUID& v) { return v.to_sstring(); });
    }
    sstring operator()(const tuple_type_impl& t, const tuple_type_impl::native_type* v) {
        return format_if_not_empty(t, v, [&t] (const tuple_type_impl::native_type& b) { return tuple_to_string(t, b); });
    }
    sstring operator()(const uuid_type_impl& u, const uuid_type_impl::native_type* v) {
        return format_if_not_empty(u, v, [] (const utils::UUID& v) { return v.to_sstring(); });
    }
    sstring operator()(const varint_type_impl& t, const varint_type_impl::native_type* v) {
        return format_if_not_empty(t, v, [] (const boost::multiprecision::cpp_int& v) { return v.str(); });
    }
};
}

static sstring to_string_impl(const abstract_type& t, const void* v) {
    return visit(t, v, to_string_impl_visitor{});
}

sstring abstract_type::to_string_impl(const data_value& v) const {
    return ::to_string_impl(*this, get_value_ptr(v));
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
            out << (*ti)->to_json_string(**vi);
        } else {
            out << "null";
        }
        ++ti;
        ++vi;
    }

    out << ']';
    return out.str();
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
        raw_tuple.emplace_back((*ti)->from_json_object(*vi, sf));
    }
    return t.build_value(std::move(raw_tuple));
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
            out << (*ti)->to_json_string(**vi);
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
    sstring operator()(const reversed_type_impl& t) { return t.underlying_type()->to_json_string(bv); }
    template <typename T> sstring operator()(const integer_type_impl<T>& t) { return to_sstring(compose_value(t, bv)); }
    template <typename T> sstring operator()(const floating_type_impl<T>& t) {
        if (bv.empty()) {
            throw exceptions::invalid_request_exception("Cannot create JSON string - deserialization error");
        }
        T d = deserialize_value(t, bv);
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
    sstring operator()(const date_type_impl& t) { return quote_json_string(t.to_string(bv)); }
    sstring operator()(const timeuuid_type_impl& t) { return quote_json_string(t.to_string(bv)); }
    sstring operator()(const timestamp_type_impl& t) { return quote_json_string(t.to_string(bv)); }
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
        return counter_cell_view::total_value_type()->to_json_string(bv);
    }
    sstring operator()(const decimal_type_impl& t) {
        if (bv.empty()) {
            throw exceptions::invalid_request_exception("Cannot create JSON string - deserialization error");
        }
        auto v = deserialize_value(t, bv);
        return v.to_string();
    }
    sstring operator()(const varint_type_impl& t) {
        if (bv.empty()) {
            throw exceptions::invalid_request_exception("Cannot create JSON string - deserialization error");
        }
        auto v = deserialize_value(t, bv);
        return v.str();
    }
};
}

sstring
abstract_type::to_json_string(bytes_view bv) const {
    return visit(*this, to_json_string_visitor{bv});
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
            raw_tuple.push_back(t->from_json_object(v, sf));
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
    bytes operator()(const reversed_type_impl& t) { return t.underlying_type()->from_json_object(value, sf); }
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
    bytes operator()(const date_type_impl& t) {
        if (!value.isString() && !value.isIntegral()) {
            throw marshal_exception("date_type must be represented as string or integer");
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
    bytes operator()(const timestamp_type_impl& t) {
        if (!value.isString() && !value.isIntegral()) {
            throw marshal_exception("uuid_type must be represented as string or integer");
        }
        if (value.isIntegral()) {
            return long_type->decompose(json::to_int64_t(value));
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

bytes abstract_type::from_json_object(const Json::Value& value, cql_serialization_format sf) const {
    return visit(*this, from_json_object_visitor{value, sf});
}

static bool
check_compatibility(const tuple_type_impl &t, const abstract_type& previous, bool (abstract_type::*predicate)(const abstract_type&) const) {
    auto* x = dynamic_cast<const tuple_type_impl*>(&previous);
    if (!x) {
        return false;
    }
    auto c = std::mismatch(
                t.all_types().begin(), t.all_types().end(),
                x->all_types().begin(), x->all_types().end(),
                [predicate] (data_type a, data_type b) { return ((*a).*predicate)(*b); });
    return c.second == x->all_types().end();  // this allowed to be longer
}

static bool is_date_long_or_timestamp(const abstract_type& t) {
    auto k = t.get_kind();
    return k == abstract_type::kind::long_kind || k == abstract_type::kind::date || k == abstract_type::kind::timestamp;
}

// Needed to handle ReversedType in value-compatibility checks.
static bool is_value_compatible_with_internal(const abstract_type& t, const abstract_type& other) {
    struct visitor {
        const abstract_type& other;
        bool operator()(const abstract_type& t) { return t.is_compatible_with(other); }
        bool operator()(const long_type_impl& t) { return is_date_long_or_timestamp(other); }
        bool operator()(const date_type_impl& t) { return is_date_long_or_timestamp(other); }
        bool operator()(const timestamp_type_impl& t) { return is_date_long_or_timestamp(other); }
        bool operator()(const uuid_type_impl&) {
            return other.get_kind() == abstract_type::kind::uuid || other.get_kind() == abstract_type::kind::timeuuid;
        }
        bool operator()(const varint_type_impl& t) {
            return other == t || int32_type->is_value_compatible_with(other) ||
                   long_type->is_value_compatible_with(other);
        }
        bool operator()(const tuple_type_impl& t) {
            return check_compatibility(t, other, &abstract_type::is_value_compatible_with);
        }
        bool operator()(const collection_type_impl& t) { return is_value_compatible_with_internal_aux(t, other); }
        bool operator()(const bytes_type_impl& t) { return true; }
        bool operator()(const reversed_type_impl& t) { return t.underlying_type()->is_value_compatible_with(other); }
    };
    return visit(t, visitor{other});
}

bool abstract_type::is_value_compatible_with(const abstract_type& other) const {
    return is_value_compatible_with_internal(*this, *other.underlying_type());
}

sstring
tuple_type_impl::make_name(const std::vector<data_type>& types) {
    // To keep format compatibility with Origin we never wrap
    // tuple name into
    // "org.apache.cassandra.db.marshal.FrozenType(...)".
    // Even when the tuple is frozen.
    // For more details see #4087
    return format("org.apache.cassandra.db.marshal.TupleType({})", ::join(", ", types | boost::adaptors::transformed(std::mem_fn(&abstract_type::name))));
}

static std::optional<std::vector<data_type>>
update_types(const std::vector<data_type> types, const user_type updated) {
    std::optional<std::vector<data_type>> new_types = std::nullopt;
    for (uint32_t i = 0; i < types.size(); ++i) {
        auto&& ut = types[i]->update_user_type(updated);
        if (ut) {
            if (!new_types) {
                new_types = types;
            }
            (*new_types)[i] = std::move(*ut);
        }
    }
    return new_types;
}

static std::optional<data_type> update_user_type_aux(
        const tuple_type_impl& t, const shared_ptr<const user_type_impl> updated) {
    if (auto new_types = update_types(t.all_types(), updated)) {
        return std::make_optional(tuple_type_impl::get_instance(std::move(*new_types)));
    }
    return std::nullopt;
}

namespace {
struct native_value_clone_visitor {
    const void* from;
    void* operator()(const reversed_type_impl& t) {
        return visit(*t.underlying_type(), native_value_clone_visitor{from});
    }
    template <typename N, typename A> void* operator()(const concrete_type<N, A>&) {
        using nt = typename concrete_type<N, A>::native_type;
        return new nt(*reinterpret_cast<const nt*>(from));
    }
    void* operator()(const counter_type_impl&) { fail(unimplemented::cause::COUNTERS); }
    void* operator()(const empty_type_impl&) {
        // Can't happen
        abort();
    }
};
}

void* abstract_type::native_value_clone(const void* from) const {
    return visit(*this, native_value_clone_visitor{from});
}

namespace {
struct native_value_delete_visitor {
    void* object;
    template <typename N, typename A> void operator()(const concrete_type<N, A>&) {
        delete reinterpret_cast<typename concrete_type<N, A>::native_type*>(object);
    }
    void operator()(const reversed_type_impl& t) {
        return visit(*t.underlying_type(), native_value_delete_visitor{object});
    }
    void operator()(const counter_type_impl&) { fail(unimplemented::cause::COUNTERS); }
    void operator()(const empty_type_impl&) {
        // Can't happen
        abort();
    }
};
}

static void native_value_delete(const abstract_type& t, void* object) {
    visit(t, native_value_delete_visitor{object});
}

namespace {
struct native_typeid_visitor {
    template <typename N, typename A> const std::type_info& operator()(const concrete_type<N, A>&) {
        return typeid(typename concrete_type<N, A>::native_type);
    }
    const std::type_info& operator()(const reversed_type_impl& t) {
        return visit(*t.underlying_type(), native_typeid_visitor{});
    }
    const std::type_info& operator()(const counter_type_impl&) { fail(unimplemented::cause::COUNTERS); }
    const std::type_info& operator()(const empty_type_impl&) {
        // Can't happen
        abort();
    }
};
}

const std::type_info& abstract_type::native_typeid() const {
    return visit(*this, native_typeid_visitor{});
}

bytes abstract_type::decompose(const data_value& value) const {
    if (!value._value) {
        return {};
    }
    bytes b(bytes::initialized_later(), serialized_size(*this, value._value));
    auto i = b.begin();
    value.serialize(i);
    return b;
}

size_t data_value::serialized_size() const {
    if (!_value) {
        return 0;
    }
    return ::serialized_size(*_type, _value);
}

void data_value::serialize(bytes::iterator& out) const {
    if (_value) {
        ::serialize(*_type, _value, out);
    }
}

bytes data_value::serialize() const {
    if (!_value) {
        return {};
    }
    bytes b(bytes::initialized_later(), serialized_size());
    auto i = b.begin();
    serialize(i);
    return b;
}

sstring abstract_type::get_string(const bytes& b) const {
    struct visitor {
        const bytes& b;
        sstring operator()(const abstract_type& t) {
            t.validate(b, cql_serialization_format::latest());
            return t.to_string(b);
        }
        sstring operator()(const reversed_type_impl& r) { return r.underlying_type()->get_string(b); }
    };
    return visit(*this, visitor{b});
}

sstring
user_type_impl::get_name_as_string() const {
    auto real_utf8_type = static_cast<const utf8_type_impl*>(utf8_type.get());
    return ::deserialize_value(*real_utf8_type, _name);
}

sstring
user_type_impl::make_name(sstring keyspace,
                          bytes name,
                          std::vector<bytes> field_names,
                          std::vector<data_type> field_types,
                          bool is_multi_cell) {
    std::ostringstream os;
    if (!is_multi_cell) {
        os << "org.apache.cassandra.db.marshal.FrozenType(";
    }
    os << "org.apache.cassandra.db.marshal.UserType(" << keyspace << "," << to_hex(name);
    for (size_t i = 0; i < field_names.size(); ++i) {
        os << ",";
        os << to_hex(field_names[i]) << ":";
        os << field_types[i]->name(); // FIXME: ignore frozen<>
    }
    os << ")";
    if (!is_multi_cell) {
        os << ")";
    }
    return os.str();
}

static std::optional<data_type> update_user_type_aux(
        const user_type_impl& u, const shared_ptr<const user_type_impl> updated) {
    if (u._keyspace == updated->_keyspace && u._name == updated->_name) {
        return std::make_optional<data_type>(updated);
    }
    if (auto new_types = update_types(u.all_types(), updated)) {
        return std::make_optional(
                user_type_impl::get_instance(u._keyspace, u._name, u.field_names(), std::move(*new_types)));
    }
    return std::nullopt;
}

std::optional<data_type> abstract_type::update_user_type(const shared_ptr<const user_type_impl> updated) const {
    struct visitor {
        const shared_ptr<const user_type_impl> updated;
        std::optional<data_type> operator()(const abstract_type&) { return std::nullopt; }
        std::optional<data_type> operator()(const empty_type_impl&) {
            return std::nullopt;
        }
        std::optional<data_type> operator()(const reversed_type_impl& r) {
            return r.underlying_type()->update_user_type(updated);
        }
        std::optional<data_type> operator()(const user_type_impl& u) { return update_user_type_aux(u, updated); }
        std::optional<data_type> operator()(const tuple_type_impl& t) { return update_user_type_aux(t, updated); }
        std::optional<data_type> operator()(const map_type_impl& m) { return update_user_type_aux(m, updated); }
        std::optional<data_type> operator()(const set_type_impl& s) {
            return update_listlike(s, set_type_impl::get_instance, updated);
        }
        std::optional<data_type> operator()(const list_type_impl& l) {
            return update_listlike(l, list_type_impl::get_instance, updated);
        }
    };
    return visit(*this, visitor{updated});
}

thread_local const shared_ptr<const abstract_type> byte_type(make_shared<byte_type_impl>());
thread_local const shared_ptr<const abstract_type> short_type(make_shared<short_type_impl>());
thread_local const shared_ptr<const abstract_type> int32_type(make_shared<int32_type_impl>());
thread_local const shared_ptr<const abstract_type> long_type(make_shared<long_type_impl>());
thread_local const shared_ptr<const abstract_type> ascii_type(make_shared<ascii_type_impl>());
thread_local const shared_ptr<const abstract_type> bytes_type(make_shared<bytes_type_impl>());
thread_local const shared_ptr<const abstract_type> utf8_type(make_shared<utf8_type_impl>());
thread_local const shared_ptr<const abstract_type> boolean_type(make_shared<boolean_type_impl>());
thread_local const shared_ptr<const abstract_type> date_type(make_shared<date_type_impl>());
thread_local const shared_ptr<const abstract_type> timeuuid_type(make_shared<timeuuid_type_impl>());
thread_local const shared_ptr<const abstract_type> timestamp_type(make_shared<timestamp_type_impl>());
thread_local const shared_ptr<const abstract_type> simple_date_type(make_shared<simple_date_type_impl>());
thread_local const shared_ptr<const abstract_type> time_type(make_shared<time_type_impl>());
thread_local const shared_ptr<const abstract_type> uuid_type(make_shared<uuid_type_impl>());
thread_local const shared_ptr<const abstract_type> inet_addr_type(make_shared<inet_addr_type_impl>());
thread_local const shared_ptr<const abstract_type> float_type(make_shared<float_type_impl>());
thread_local const shared_ptr<const abstract_type> double_type(make_shared<double_type_impl>());
thread_local const shared_ptr<const abstract_type> varint_type(make_shared<varint_type_impl>());
thread_local const shared_ptr<const abstract_type> decimal_type(make_shared<decimal_type_impl>());
thread_local const shared_ptr<const abstract_type> counter_type(make_shared<counter_type_impl>());
thread_local const shared_ptr<const abstract_type> duration_type(make_shared<duration_type_impl>());
thread_local const data_type empty_type(make_shared<empty_type_impl>());

data_type abstract_type::parse_type(const sstring& name)
{
    static thread_local const std::unordered_map<sstring, data_type> types = {
        { byte_type_name,      byte_type      },
        { short_type_name,     short_type     },
        { int32_type_name,     int32_type     },
        { long_type_name,      long_type      },
        { ascii_type_name,     ascii_type     },
        { bytes_type_name,     bytes_type     },
        { utf8_type_name,      utf8_type      },
        { boolean_type_name,   boolean_type   },
        { date_type_name,      date_type      },
        { timeuuid_type_name,  timeuuid_type  },
        { timestamp_type_name, timestamp_type },
        { simple_date_type_name, simple_date_type },
        { time_type_name,      time_type },
        { uuid_type_name,      uuid_type      },
        { inet_addr_type_name, inet_addr_type },
        { float_type_name,     float_type     },
        { double_type_name,    double_type    },
        { varint_type_name,    varint_type    },
        { decimal_type_name,   decimal_type   },
        { counter_type_name,   counter_type   },
        { duration_type_name,  duration_type  },
        { empty_type_name,     empty_type     },
    };
    auto it = types.find(name);
    if (it == types.end()) {
        throw std::invalid_argument(format("unknown type: {}\n", name));
    }
    return it->second;
}

data_value::~data_value() {
    if (_value) {
        native_value_delete(*_type, _value);
    }
}

data_value::data_value(const data_value& v) : _value(nullptr), _type(v._type) {
    if (v._value) {
        _value = _type->native_value_clone(v._value);
    }
}

data_value&
data_value::operator=(data_value&& x) {
    auto tmp = std::move(x);
    std::swap(tmp._value, this->_value);
    std::swap(tmp._type, this->_type);
    return *this;
}

data_value::data_value(bytes v) : data_value(make_new(bytes_type, v)) {
}

data_value::data_value(sstring v) : data_value(make_new(utf8_type, v)) {
}

data_value::data_value(const char* v) : data_value(make_new(utf8_type, sstring(v))) {
}

data_value::data_value(ascii_native_type v) : data_value(make_new(ascii_type, v.string)) {
}

data_value::data_value(bool v) : data_value(make_new(boolean_type, v)) {
}

data_value::data_value(int8_t v) : data_value(make_new(byte_type, v)) {
}

data_value::data_value(int16_t v) : data_value(make_new(short_type, v)) {
}

data_value::data_value(int32_t v) : data_value(make_new(int32_type, v)) {
}

data_value::data_value(int64_t v) : data_value(make_new(long_type, v)) {
}

data_value::data_value(utils::UUID v) : data_value(make_new(uuid_type, v)) {
}

data_value::data_value(float v) : data_value(make_new(float_type, v)) {
}

data_value::data_value(double v) : data_value(make_new(double_type, v)) {
}

data_value::data_value(seastar::net::inet_address v) : data_value(make_new(inet_addr_type, v)) {
}

data_value::data_value(seastar::net::ipv4_address v) : data_value(seastar::net::inet_address(v)) {
}
data_value::data_value(seastar::net::ipv6_address v) : data_value(seastar::net::inet_address(v)) {
}

data_value::data_value(simple_date_native_type v) : data_value(make_new(simple_date_type, v.days)) {
}

data_value::data_value(timestamp_native_type v) : data_value(make_new(timestamp_type, v.tp)) {
}

data_value::data_value(time_native_type v) : data_value(make_new(time_type, v.nanoseconds)) {
}

data_value::data_value(timeuuid_native_type v) : data_value(make_new(timeuuid_type, v.uuid)) {
}

data_value::data_value(db_clock::time_point v) : data_value(make_new(date_type, v)) {
}

data_value::data_value(boost::multiprecision::cpp_int v) : data_value(make_new(varint_type, v)) {
}

data_value::data_value(big_decimal v) : data_value(make_new(decimal_type, v)) {
}

data_value::data_value(cql_duration d) : data_value(make_new(duration_type, d)) {
}

data_value
make_list_value(data_type type, list_type_impl::native_type value) {
    return data_value::make_new(std::move(type), std::move(value));
}

data_value
make_set_value(data_type type, set_type_impl::native_type value) {
    return data_value::make_new(std::move(type), std::move(value));
}

data_value
make_map_value(data_type type, map_type_impl::native_type value) {
    return data_value::make_new(std::move(type), std::move(value));
}

data_value
make_tuple_value(data_type type, tuple_type_impl::native_type value) {
    return data_value::make_new(std::move(type), std::move(value));
}

data_value
make_user_value(data_type type, user_type_impl::native_type value) {
    return data_value::make_new(std::move(type), std::move(value));
}

std::ostream& operator<<(std::ostream& out, const data_value& v) {
    if (v.is_null()) {
        return out << "null";
    }
    return out << v.type()->to_string_impl(v);
}

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
        boost::multiprecision::cpp_int ten(10);
        boost::multiprecision::cpp_rational r = val_from.unscaled_value();
        r /= boost::multiprecision::pow(ten, val_from.scale());
        return static_cast<ToType>(r);
    };
}

template<typename ToType>
std::function<data_value(data_value)> make_castas_fctn_from_decimal_to_integer() {
    return [](data_value from) -> data_value {
        auto val_from = value_cast<big_decimal>(from);
        boost::multiprecision::cpp_int ten(10);
        return static_cast<ToType>(val_from.unscaled_value() / boost::multiprecision::pow(ten, val_from.scale()));
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
        return to_sstring(value_cast<boost::multiprecision::cpp_int>(from).str());
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
        return utils::UUID_gen::unix_timestamp(val_from);
    };
}

std::function<data_value(data_value)> make_castas_fctn_from_timeuuid_to_date() {
    return [](data_value from) -> data_value {
        const auto val_from = value_cast<utils::UUID>(from);
        return time_point_to_date(millis_to_time_point(utils::UUID_gen::unix_timestamp(val_from)));
    };
}

std::function<data_value(data_value)> make_castas_fctn_from_timestamp_to_string() {
    return [](data_value from) -> data_value {
        const auto val_from = value_cast<db_clock::time_point>(from);
        return time_point_to_string(val_from);
    };
}

std::function<data_value(data_value)> make_castas_fctn_from_simple_date_to_string() {
    return [](data_value from) -> data_value {
        return simple_date_to_string(value_cast<uint32_t>(from));
    };
}

std::function<data_value(data_value)> make_castas_fctn_from_time_to_string() {
    return [](data_value from) -> data_value {
        return time_to_string(value_cast<int64_t>(from));
    };
}

std::function<data_value(data_value)> make_castas_fctn_from_uuid_to_string() {
    return [](data_value from) -> data_value {
        return value_cast<utils::UUID>(from).to_sstring();
    };
}

std::function<data_value(data_value)> make_castas_fctn_from_boolean_to_string() {
    return [](data_value from) -> data_value {
        return boolean_to_string(value_cast<bool>(from));
    };
}

std::function<data_value(data_value)> make_castas_fctn_from_inet_to_string() {
    return [](data_value from) -> data_value {
        return inet_to_string(value_cast<inet_address>(from));
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
    { {byte_type, varint_type}, make_castas_fctn_simple<int8_t, boost::multiprecision::cpp_int>() },
    { {byte_type, decimal_type}, make_castas_fctn_from_decimal_to_float<int8_t>() },

    { {short_type, byte_type}, make_castas_fctn_simple<int16_t, int8_t>() },
    { {short_type, short_type}, make_castas_fctn_simple<int16_t, int16_t>() },
    { {short_type, int32_type}, make_castas_fctn_simple<int16_t, int32_t>() },
    { {short_type, long_type}, make_castas_fctn_simple<int16_t, int64_t>() },
    { {short_type, float_type}, make_castas_fctn_simple<int16_t, float>() },
    { {short_type, double_type}, make_castas_fctn_simple<int16_t, double>() },
    { {short_type, varint_type}, make_castas_fctn_simple<int16_t, boost::multiprecision::cpp_int>() },
    { {short_type, decimal_type}, make_castas_fctn_from_decimal_to_float<int16_t>() },

    { {int32_type, byte_type}, make_castas_fctn_simple<int32_t, int8_t>() },
    { {int32_type, short_type}, make_castas_fctn_simple<int32_t, int16_t>() },
    { {int32_type, int32_type}, make_castas_fctn_simple<int32_t, int32_t>() },
    { {int32_type, long_type}, make_castas_fctn_simple<int32_t, int64_t>() },
    { {int32_type, float_type}, make_castas_fctn_simple<int32_t, float>() },
    { {int32_type, double_type}, make_castas_fctn_simple<int32_t, double>() },
    { {int32_type, varint_type}, make_castas_fctn_simple<int32_t, boost::multiprecision::cpp_int>() },
    { {int32_type, decimal_type}, make_castas_fctn_from_decimal_to_float<int32_t>() },

    { {long_type, byte_type}, make_castas_fctn_simple<int64_t, int8_t>() },
    { {long_type, short_type}, make_castas_fctn_simple<int64_t, int16_t>() },
    { {long_type, int32_type}, make_castas_fctn_simple<int64_t, int32_t>() },
    { {long_type, long_type}, make_castas_fctn_simple<int64_t, int64_t>() },
    { {long_type, float_type}, make_castas_fctn_simple<int64_t, float>() },
    { {long_type, double_type}, make_castas_fctn_simple<int64_t, double>() },
    { {long_type, varint_type}, make_castas_fctn_simple<int64_t, boost::multiprecision::cpp_int>() },
    { {long_type, decimal_type}, make_castas_fctn_from_decimal_to_float<int64_t>() },

    { {float_type, byte_type}, make_castas_fctn_simple<float, int8_t>() },
    { {float_type, short_type}, make_castas_fctn_simple<float, int16_t>() },
    { {float_type, int32_type}, make_castas_fctn_simple<float, int32_t>() },
    { {float_type, long_type}, make_castas_fctn_simple<float, int64_t>() },
    { {float_type, float_type}, make_castas_fctn_simple<float, float>() },
    { {float_type, double_type}, make_castas_fctn_simple<float, double>() },
    { {float_type, varint_type}, make_castas_fctn_simple<float, boost::multiprecision::cpp_int>() },
    { {float_type, decimal_type}, make_castas_fctn_from_decimal_to_float<float>() },

    { {double_type, byte_type}, make_castas_fctn_simple<double, int8_t>() },
    { {double_type, short_type}, make_castas_fctn_simple<double, int16_t>() },
    { {double_type, int32_type}, make_castas_fctn_simple<double, int32_t>() },
    { {double_type, long_type}, make_castas_fctn_simple<double, int64_t>() },
    { {double_type, float_type}, make_castas_fctn_simple<double, float>() },
    { {double_type, double_type}, make_castas_fctn_simple<double, double>() },
    { {double_type, varint_type}, make_castas_fctn_simple<double, boost::multiprecision::cpp_int>() },
    { {double_type, decimal_type}, make_castas_fctn_from_decimal_to_float<double>() },

    { {varint_type, byte_type}, make_castas_fctn_simple<boost::multiprecision::cpp_int, int8_t>() },
    { {varint_type, short_type}, make_castas_fctn_simple<boost::multiprecision::cpp_int, int16_t>() },
    { {varint_type, int32_type}, make_castas_fctn_simple<boost::multiprecision::cpp_int, int32_t>() },
    { {varint_type, long_type}, make_castas_fctn_simple<boost::multiprecision::cpp_int, int64_t>() },
    { {varint_type, float_type}, make_castas_fctn_simple<boost::multiprecision::cpp_int, float>() },
    { {varint_type, double_type}, make_castas_fctn_simple<boost::multiprecision::cpp_int, double>() },
    { {varint_type, varint_type}, make_castas_fctn_simple<boost::multiprecision::cpp_int, boost::multiprecision::cpp_int>() },
    { {varint_type, decimal_type}, make_castas_fctn_from_decimal_to_integer<boost::multiprecision::cpp_int>() },

    { {decimal_type, byte_type}, make_castas_fctn_from_integer_to_decimal<int8_t>() },
    { {decimal_type, short_type}, make_castas_fctn_from_integer_to_decimal<int16_t>() },
    { {decimal_type, int32_type}, make_castas_fctn_from_integer_to_decimal<int32_t>() },
    { {decimal_type, long_type}, make_castas_fctn_from_integer_to_decimal<int64_t>() },
    { {decimal_type, float_type}, make_castas_fctn_from_float_to_decimal<float>() },
    { {decimal_type, double_type}, make_castas_fctn_from_float_to_decimal<double>() },
    { {decimal_type, varint_type}, make_castas_fctn_from_integer_to_decimal<boost::multiprecision::cpp_int>() },
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

    { {ascii_type, timestamp_type}, make_castas_fctn_from_timestamp_to_string() },
    { {ascii_type, simple_date_type}, make_castas_fctn_from_simple_date_to_string() },
    { {ascii_type, time_type}, make_castas_fctn_from_time_to_string() },
    { {ascii_type, timeuuid_type}, make_castas_fctn_from_uuid_to_string() },
    { {ascii_type, uuid_type}, make_castas_fctn_from_uuid_to_string() },
    { {ascii_type, boolean_type}, make_castas_fctn_from_boolean_to_string() },
    { {ascii_type, inet_addr_type}, make_castas_fctn_from_inet_to_string() },
    { {ascii_type, ascii_type}, make_castas_fctn_simple<sstring, sstring>() },

    { {utf8_type, timestamp_type}, make_castas_fctn_from_timestamp_to_string() },
    { {utf8_type, simple_date_type}, make_castas_fctn_from_simple_date_to_string() },
    { {utf8_type, time_type}, make_castas_fctn_from_time_to_string() },
    { {utf8_type, timeuuid_type}, make_castas_fctn_from_uuid_to_string() },
    { {utf8_type, uuid_type}, make_castas_fctn_from_uuid_to_string() },
    { {utf8_type, boolean_type}, make_castas_fctn_from_boolean_to_string() },
    { {utf8_type, boolean_type}, make_castas_fctn_from_boolean_to_string() },
    { {utf8_type, inet_addr_type}, make_castas_fctn_from_inet_to_string() },
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
