/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include <boost/lexical_cast.hpp>
#include <algorithm>
#include "cql3/cql3_type.hh"
#include "cql3/description.hh"
#include "cql3/lists.hh"
#include "cql3/maps.hh"
#include "cql3/sets.hh"
#include "cql3/util.hh"
#include "concrete_types.hh"
#include <exception>
#include <iterator>
#include <seastar/core/format.hh>
#include <seastar/core/shared_ptr.hh>
#include "types/types.hh"
#include "utils/assert.hh"
#include "utils/serialization.hh"
#include "vint-serialization.hh"
#include <cmath>
#include <chrono>
#include <sstream>
#include <string>
#include <boost/regex.hpp>
#include <concepts>
#include <ctime>
#include <cstdlib>
#include <fmt/chrono.h>
#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/date_time/c_local_time_adjustor.hpp>
#include <boost/locale/encoding_utf.hpp>
#include <boost/multiprecision/cpp_int.hpp>
#include <seastar/net/inet_address.hh>
#include <unordered_set>
#include "utils/big_decimal.hh"
#include "utils/date.h"
#include "utils/utf8.hh"
#include "utils/ascii.hh"
#include "utils/fragment_range.hh"
#include "utils/managed_bytes.hh"

#include "types/user.hh"
#include "types/tuple.hh"
#include "types/vector.hh"
#include "types/collection.hh"
#include "types/map.hh"
#include "types/list.hh"
#include "types/set.hh"
#include "types/listlike_partial_deserializing_iterator.hh"

static logging::logger tlogger("types");

bytes_view_opt read_collection_value(bytes_view& in);

void on_types_internal_error(std::exception_ptr ex) {
    on_internal_error(tlogger, std::move(ex));
}

template<typename T>
requires requires {
        typename T::duration;
        requires std::same_as<typename T::duration, std::chrono::milliseconds>;
    }
sstring
time_point_to_string(const T& tp, bool use_time_separator = true)
{
    auto count = tp.time_since_epoch().count();
    auto d = std::div(int64_t(count), int64_t(1000));
    std::time_t seconds = d.quot;
    std::tm tm;
    if (!gmtime_r(&seconds, &tm)) {
        return fmt::format("{} milliseconds (out of range)", count);
    }

    auto millis = d.rem;
    // adjust seconds for time points earlier than posix epoch
    // to keep the fractional millis positive
    if (millis < 0) {
        millis += 1000;
        seconds--;
        gmtime_r(&seconds, &tm);
    }

    const auto time_separator = (use_time_separator) ? "T" : " ";
    auto year_digits = tm.tm_year >= -1900 ? 4 : 5;

    return fmt::format("{:-0{}d}-{:02d}-{:02d}{}{:02d}:{:02d}:{:02d}.{:03d}Z",
            tm.tm_year + 1900, year_digits, tm.tm_mon + 1, tm.tm_mday, time_separator,
            tm.tm_hour, tm.tm_min, tm.tm_sec, millis);
}

sstring simple_date_to_string(const uint32_t days_count) {
    date::days days{days_count - (1UL << 31)};
    date::year_month_day ymd{date::local_days{days}};
    std::ostringstream str;
    str << ymd;
    return std::move(str).str();
}

sstring time_to_string(const int64_t nanoseconds_count) {
    std::string s;
    std::chrono::nanoseconds nanoseconds{nanoseconds_count};
    fmt::format_to(std::back_inserter(s), "{:%H:%M:%S}", nanoseconds);
    return s;
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
    static T read_nonempty(managed_bytes_view v) {
        return read_simple_exactly<T>(v);
    }
};

template<>
struct simple_type_traits<bool> {
    static constexpr size_t serialized_size = 1;
    static bool read_nonempty(managed_bytes_view v) {
        return read_simple_exactly<int8_t>(v) != 0;
    }
};

template<>
struct simple_type_traits<db_clock::time_point> {
    static constexpr size_t serialized_size = sizeof(uint64_t);
    static db_clock::time_point read_nonempty(managed_bytes_view v) {
        return db_clock::time_point(db_clock::duration(read_simple_exactly<int64_t>(v)));
    }
};

template <typename T>
simple_type_impl<T>::simple_type_impl(abstract_type::kind k, sstring name, std::optional<uint32_t> value_length_if_fixed)
    : concrete_type<T>(k, std::move(name), std::move(value_length_if_fixed)) {}

template <typename T>
integer_type_impl<T>::integer_type_impl(
        abstract_type::kind k, sstring name, std::optional<uint32_t> value_length_if_fixed)
    : simple_type_impl<T>(k, name, std::move(value_length_if_fixed)) {}

template <typename T> static bytes decompose_value(T v) {
    bytes b(bytes::initialized_later(), sizeof(v));
    write_unaligned<T>(b.begin(), net::hton(v));
    return b;
}

template <typename T> static T parse_int(const integer_type_impl<T>& t, std::string_view s) {
    try {
        auto value64 = boost::lexical_cast<int64_t>(s.begin(), s.size());
        auto value = static_cast<T>(value64);
        if (value != value64) {
            throw marshal_exception(seastar::format("Value out of range for type {}: '{}'", t.name(), s));
        }
        return static_cast<T>(value);
    } catch (const boost::bad_lexical_cast& e) {
        throw marshal_exception(seastar::format("Invalid number format '{}'", s));
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
    : concrete_type(k, name, {}) {}

ascii_type_impl::ascii_type_impl() : string_type_impl(kind::ascii, ascii_type_name) {}

utf8_type_impl::utf8_type_impl() : string_type_impl(kind::utf8, utf8_type_name) {}

bytes_type_impl::bytes_type_impl()
    : concrete_type(kind::bytes, bytes_type_name, {}) {}

boolean_type_impl::boolean_type_impl() : simple_type_impl<bool>(kind::boolean, boolean_type_name, 1) {}

date_type_impl::date_type_impl() : concrete_type(kind::date, date_type_name, 8) {}

timeuuid_type_impl::timeuuid_type_impl()
    : concrete_type<utils::UUID>(
              kind::timeuuid, timeuuid_type_name, 16) {}

timestamp_type_impl::timestamp_type_impl() : simple_type_impl(kind::timestamp, timestamp_type_name, 8) {}

static boost::posix_time::ptime get_time(const boost::smatch& sm) {
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

int64_t timestamp_from_string(std::string_view s) {
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

        static const boost::regex date_re("^(\\d{4})-(\\d+)-(\\d+)([ tT](\\d+):(\\d+)(:(\\d+)(\\.(\\d+))?)?)?");
        boost::smatch dsm;
        if (!boost::regex_search(str, dsm, date_re)) {
            throw marshal_exception(seastar::format("Unable to parse timestamp from '{}'", str));
        }
        auto t = get_time(dsm);

        auto tz = dsm.suffix().str();
        const auto str_begin = tz.find_first_not_of(" ");
        bool is_tz_empty = str_begin == std::string::npos;

        std::string_view tz_trim = tz;
        if (!is_tz_empty) {
            tz_trim.remove_prefix(str_begin);
            const auto str_end = tz_trim.find_last_not_of(" ");
            tz_trim.remove_suffix(tz_trim.size() - str_end - 1);
        }

        static const boost::regex tz_re("([\\+-]\\d{2}:?(\\d{2})?)");
        boost::smatch tsm;
        if (boost::regex_match(tz, tsm, tz_re)) {
            t -= get_utc_offset(tsm.str());
        } else if (is_tz_empty) {
            typedef boost::date_time::c_local_adjustor<boost::posix_time::ptime> local_tz;
            // local_tz::local_to_utc(), where are you?
            auto t1 = local_tz::utc_to_local(t);
            auto tz_offset = t1 - t;
            auto t2 = local_tz::utc_to_local(t - tz_offset);
            auto dst_offset = t2 - t;
            t -= tz_offset + dst_offset;
        } else if (tz_trim != "z" && tz_trim != "utc" && tz_trim != "gmt") {
            throw marshal_exception(seastar::format("Unable to parse timezone '{}'", tz));
        }
        return (t - boost::posix_time::from_time_t(0)).total_milliseconds();
    } catch (const marshal_exception& me) {
        throw marshal_exception(
            seastar::format("unable to parse date '{}': {}", s, me.what()));
    } catch (...) {
        throw marshal_exception(seastar::format("unable to parse date '{}': {}", s, std::current_exception()));
    }
}

db_clock::time_point timestamp_type_impl::from_string_view(std::string_view s) {
    return db_clock::time_point(db_clock::duration(timestamp_from_string(s)));
}

simple_date_type_impl::simple_date_type_impl() : simple_type_impl{kind::simple_date, simple_date_type_name, {}} {}

template <typename ConstIterator>
requires requires (ConstIterator it) {
    { it[0] } -> std::same_as<const char&>;
}
static date::year_month_day get_simple_date_time(const boost::match_results<ConstIterator>& sm) {
    auto year = boost::lexical_cast<long>(sm[1]);
    auto month = boost::lexical_cast<unsigned>(sm[2]);
    auto day = boost::lexical_cast<unsigned>(sm[3]);
    return date::year_month_day{date::year{year}, date::month{month}, date::day{day}};
}
static uint32_t serialize(std::string_view input, int64_t days) {
    if (days < std::numeric_limits<int32_t>::min()) {
        throw marshal_exception(seastar::format("Input date {} is less than min supported date -5877641-06-23", input));
    }
    if (days > std::numeric_limits<int32_t>::max()) {
        throw marshal_exception(seastar::format("Input date {} is greater than max supported date 5881580-07-11", input));
    }
    days += 1UL << 31;
    return static_cast<uint32_t>(days);
}
uint32_t simple_date_type_impl::from_string_view(std::string_view s) {
    char* end;
    errno = 0;
    auto v = std::strtoll(s.begin(), &end, 10);
    if(end != s.end()) {
        static const boost::regex date_re("^(-?\\d+)-(\\d+)-(\\d+)");
        boost::match_results<std::string_view::const_iterator> dsm;
        if (!boost::regex_match(s.begin(), s.end(), dsm, date_re)) {
        throw marshal_exception(seastar::format("Unable to coerce '{}' to a formatted date (long)", s));
        }
        auto t = get_simple_date_time(dsm);
        return serialize(s, date::local_days(t).time_since_epoch().count());
    }
    if (errno == ERANGE) {
        throw marshal_exception(format("Unable to make unsigned int (for date) from {}", v));
    }
    if (v < std::numeric_limits<uint32_t>::min() || v > std::numeric_limits<uint32_t>::max()) {
        throw marshal_exception(format("Unable to make unsigned int (for date) from {}", v));
    }
    return v;
}

time_type_impl::time_type_impl() : simple_type_impl{kind::time, time_type_name, {}} {}

int64_t time_type_impl::from_string_view(std::string_view s) {
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
        auto nano_digits = s.length() - (seconds_end + 1);
        if (nano_digits > 9) {
            throw marshal_exception(seastar::format("more than 9 nanosecond digits: {}", s));
        }
        nanoseconds *= std::pow(10, 9 - nano_digits);
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
    : concrete_type(kind::uuid, uuid_type_name, 16) {}

using inet_address = seastar::net::inet_address;

inet_addr_type_impl::inet_addr_type_impl()
    : concrete_type<inet_address>(kind::inet, inet_addr_type_name, {}) {}

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
    static double read_nonempty(managed_bytes_view v) {
        return std::bit_cast<T>(read_simple_exactly<typename int_of_size<T>::itype>(v));
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

varint_type_impl::varint_type_impl() : concrete_type{kind::varint, varint_type_name, { }} { }

decimal_type_impl::decimal_type_impl() : concrete_type{kind::decimal, decimal_type_name, { }} { }

counter_type_impl::counter_type_impl()
    : abstract_type{kind::counter, counter_type_name, {}} {}

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
    : concrete_type(kind::duration, duration_type_name, {}) {}

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
    : abstract_type(kind::empty, empty_type_name, 0) {}

logging::logger collection_type_impl::_logger("collection_type_impl");
const size_t collection_type_impl::max_elements;

lw_shared_ptr<cql3::column_specification> collection_type_impl::make_collection_receiver(
        const cql3::column_specification& collection, bool is_key) const {
    struct visitor {
        const cql3::column_specification& collection;
        bool is_key;
        lw_shared_ptr<cql3::column_specification> operator()(const abstract_type&) { abort(); }
        lw_shared_ptr<cql3::column_specification> operator()(const list_type_impl&) {
            return cql3::lists::value_spec_of(collection);
        }
        lw_shared_ptr<cql3::column_specification> operator()(const map_type_impl&) {
            return is_key ? cql3::maps::key_spec_of(collection) : cql3::maps::value_spec_of(collection);
        }
        lw_shared_ptr<cql3::column_specification> operator()(const set_type_impl&) {
            return cql3::sets::value_spec_of(collection);
        }
    };
    return ::visit(*this, visitor{collection, is_key});
}

listlike_collection_type_impl::listlike_collection_type_impl(
        kind k, sstring name, data_type elements, bool is_multi_cell)
    : collection_type_impl(k, name, is_multi_cell), _elements(elements) {}

std::strong_ordering listlike_collection_type_impl::compare_with_map(const map_type_impl& map_type, bytes_view list, bytes_view map) const
{
    SCYLLA_ASSERT((is_set() && map_type.get_keys_type() == _elements) || (!is_set() && map_type.get_values_type() == _elements));

    if (list.empty()) {
        return map.empty() ? std::strong_ordering::equal : std::strong_ordering::less;
    } else if (map.empty()) {
        return std::strong_ordering::greater;
    }

    const abstract_type& element_type = *_elements;

    size_t list_size = read_collection_size(list);
    size_t map_size = read_collection_size(map);

    bytes_view_opt list_value;
    bytes_view_opt map_value[2];

    // Lists are represented as vector<pair<timeuuid, value>>, sets are vector<pair<value, empty>>
    size_t map_value_index = is_list();
    // Both set elements and map keys are sorted, so can be compared in linear order;
    // List elements are stored in both vectors in list index order.
    for (size_t i = 0; i < std::min(list_size, map_size); ++i) {

        list_value = read_collection_value_nonnull(list);
        map_value[0] = read_collection_value_nonnull(map);
        // sets-as-maps happen to be serialized with NULL
        map_value[1] = read_collection_value(map);
        if (!list_value) {
            return std::strong_ordering::less;
        }
        // map_value[0] is known non-null, and sets will compare map_value[0].
        auto cmp = element_type.compare(*list_value, *map_value[map_value_index]);
        if (cmp != 0) {
            return cmp;
        }
    }
    return list_size <=> map_size;
}

bytes listlike_collection_type_impl::serialize_map(const map_type_impl& map_type, const data_value& value) const
{
    SCYLLA_ASSERT((is_set() && map_type.get_keys_type() == _elements) || (!is_set() && map_type.get_values_type() == _elements));
    const std::vector<std::pair<data_value, data_value>>& map = map_type.from_value(value);
    // Lists are represented as vector<pair<timeuuid, value>>, sets are vector<pair<value, empty>>
    bool first = is_set();

    size_t len = collection_size_len();
    size_t psz = collection_value_len();
    for (const std::pair<data_value, data_value>& entry : map) {
        len += psz + (first ? entry.first : entry.second).serialized_size();
    }

    bytes b(bytes::initialized_later(), len);
    bytes::iterator out = b.begin();

    write_collection_size(out, map.size());
    for (const std::pair<data_value, data_value>& entry : map) {
        write_collection_value(out, _elements, first ? entry.first : entry.second);
    }
    return b;
}

void
listlike_collection_type_impl::validate_for_storage(const FragmentedView auto& value) const {
    for (auto val_opt : partially_deserialize_listlike(value)) {
        if (!val_opt) {
            throw exceptions::invalid_request_exception("Cannot store NULL in list or set");
        }
    }
}

template
void listlike_collection_type_impl::validate_for_storage(const managed_bytes_view& value) const;

template
void listlike_collection_type_impl::validate_for_storage(const fragmented_temporary_buffer::view& value) const;


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

size_t collection_size_len() {
    return sizeof(int32_t);
}

size_t collection_value_len() {
    return sizeof(int32_t);
}


int read_collection_size(bytes_view& in) {
    return read_simple<int32_t>(in);
}

void write_collection_size(bytes::iterator& out, int size) {
    serialize_int32(out, size);
}

bytes_view read_collection_value_nonnull(bytes_view& in) {
    int32_t size = read_simple<int32_t>(in);
    if (size == -2) {
        throw exceptions::invalid_request_exception("unset value is not supported inside collections");
    }
    if (size < 0) {
        throw exceptions::invalid_request_exception("null is not supported inside collections");
    }
    return read_simple_bytes(in, size);
}

bytes_view read_collection_key(bytes_view& in) {
    int32_t size = read_simple<int32_t>(in);
    if (size < 0) {
        throw exceptions::invalid_request_exception("null/unset is not supported inside collections");
    }
    return read_simple_bytes(in, size);
}
bytes_view_opt read_collection_value(bytes_view& in) {
    int32_t size = read_simple<int32_t>(in);
    if (size == -1) {
        throw std::nullopt;
    }
    if (size == -2) {
        throw exceptions::invalid_request_exception("unset value is not supported inside collections");
    }
    if (size < 0) {
        throw exceptions::invalid_request_exception("null is not supported inside collections");
    }
    return read_simple_bytes(in, size);
}

void write_collection_value(bytes::iterator& out, std::optional<bytes_view> val_bytes_opt) {
    if (!val_bytes_opt) {
        serialize_int32(out, int32_t(-1));
        return;
    }
    auto& val_bytes = *val_bytes_opt;
    serialize_int32(out, int32_t(val_bytes.size()));
    out = std::copy_n(val_bytes.begin(), val_bytes.size(), out);
}

// Passing the wrong integer type to a generic serialization function is a particularly
// easy mistake to do, so we want to disable template parameter deduction here.
// Hence std::type_identity.
template<typename T>
void write_simple(bytes_ostream& out, std::type_identity_t<T> val) {
    auto val_be = net::hton(val);
    auto val_ptr = reinterpret_cast<const bytes::value_type*>(&val_be);
    out.write(bytes_view(val_ptr, sizeof(T)));
}

void write_collection_value(bytes_ostream& out, atomic_cell_value_view val) {
    write_simple<int32_t>(out, int32_t(val.size_bytes()));
    for (auto&& frag : fragment_range(val)) {
        out.write(frag);
    }
}

void write_fragmented(managed_bytes_mutable_view& out, std::string_view val) {
    while (val.size() > 0) {
        size_t current_n = std::min(val.size(), out.current_fragment().size());
        memcpy(out.current_fragment().data(), val.data(), current_n);
        val.remove_prefix(current_n);
        out.remove_prefix(current_n);
    }
}

template<std::integral T>
void write_simple(managed_bytes_mutable_view& out, std::type_identity_t<T> val) {
    val = net::hton(val);
    if (out.current_fragment().size() >= sizeof(T)) [[likely]] {
        auto p = out.current_fragment().data();
        out.remove_prefix(sizeof(T));
        // FIXME use write_unaligned after it's merged.
        write_unaligned<T>(p, val);
    } else if (out.size_bytes() >= sizeof(T)) {
        write_fragmented(out, std::string_view(reinterpret_cast<const char*>(&val), sizeof(T)));
    } else {
        on_internal_error(tlogger, format("write_simple: attempted write of size {} to buffer of size {}", sizeof(T), out.size_bytes()));
    }
}

void write_collection_size(managed_bytes_mutable_view& out, int size) {
    write_simple<uint32_t>(out, uint32_t(size));
}

void write_collection_value(managed_bytes_mutable_view& out, bytes_view val) {
    write_simple<int32_t>(out, int32_t(val.size()));
    write_fragmented(out, single_fragmented_view(val));
}

void write_collection_value(managed_bytes_mutable_view& out, const managed_bytes_view_opt& val_opt) {
    if (!val_opt) {
        write_simple<int32_t>(out, int32_t(-1));
        return;
    }
    auto& val = *val_opt;
    write_simple<int32_t>(out, int32_t(val.size_bytes()));
    write_fragmented(out, val);
}

void write_int32(bytes::iterator& out, int32_t value) {
    return serialize_int32(out, value);
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
        bool operator()(const reversed_type_impl& t) { return t.underlying_type()->is_tuple(); }
        bool operator()(const tuple_type_impl&) { return true; }
    };
    return visit(*this, visitor{});
}

bool abstract_type::is_vector() const {
    struct visitor {
        bool operator()(const abstract_type&) { return false; }
        bool operator()(const reversed_type_impl& t) { return t.underlying_type()->is_vector(); }
        bool operator()(const vector_type_impl&) { return true; }
    };
    return visit(*this, visitor{});
}

bool abstract_type::is_multi_cell() const {
    struct visitor {
        bool operator()(const abstract_type&) { return false; }
        bool operator()(const reversed_type_impl& t) { return t.underlying_type()->is_multi_cell(); }
        bool operator()(const collection_type_impl& c) { return c.is_multi_cell(); }
        bool operator()(const user_type_impl& u) { return u.is_multi_cell(); }
    };
    return visit(*this, visitor{});
}

bool abstract_type::is_native() const { return !is_collection() && !is_tuple() && !is_vector(); }

bool abstract_type::is_string() const {
    struct visitor {
        bool operator()(const abstract_type&) { return false; }
        bool operator()(const reversed_type_impl& t) { return t.underlying_type()->is_string(); }
        bool operator()(const string_type_impl&) { return true; }
    };
    return visit(*this, visitor{});
}

template<typename Predicate>
requires CanHandleAllTypes<Predicate>
static bool find(const abstract_type& t, const Predicate& f) {
    struct visitor {
        const Predicate& f;
        bool operator()(const abstract_type&) { return false; }
        bool operator()(const reversed_type_impl& r) { return find(*r.underlying_type(), f); }
        bool operator()(const tuple_type_impl& t) {
            return std::ranges::any_of(t.all_types(), [&] (const data_type& dt) { return find(*dt, f); });
        }
        bool operator()(const vector_type_impl& t) { return find(*t.get_elements_type(), f); }
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
// For tuples and collection types: recurse over inner types
// For user-defined types: add it's name to referenced udts
// For other types: do nothing
struct get_all_referenced_user_types_visitor {
    std::set<user_type> referenced_udts;

    template<typename T>
    void operator()(const concrete_type<T>&) { return; }
    void operator()(const counter_type_impl&) { return; }
    void operator()(const empty_type_impl&) { return; }
    void operator()(const reversed_type_impl& r) {
        visit(*r.underlying_type(), *this);
    }
    void operator()(const user_type_impl& u) {
        referenced_udts.insert(::static_pointer_cast<const user_type_impl>(u.shared_from_this()));
    }
    void operator()(const tuple_type_impl& t) {
        for (auto& field: t.all_types()) {
            visit(*field, *this);
        }
    }
    void operator()(const vector_type_impl& v) {
        visit(*v.get_elements_type(), *this);
    }
    void operator()(const map_type_impl& m) {
        visit(*m.get_keys_type(), *this);
        visit(*m.get_values_type(), *this);
    }
    void operator()(const listlike_collection_type_impl& l) {
        visit(*l.get_elements_type(), *this);
    }
};
}

std::set<user_type> user_type_impl::get_all_referenced_user_types() const {
    get_all_referenced_user_types_visitor v;
    // cast this UDT to tuple to iterate over all fields
    v(static_cast<const tuple_type_impl&>(*this));
    return std::move(v.referenced_udts);
}

namespace {
struct is_byte_order_equal_visitor {
    template <typename T> bool operator()(const simple_type_impl<T>&) { return true; }
    bool operator()(const concrete_type<utils::UUID>&) { return true; }
    bool operator()(const abstract_type&) { return false; }
    bool operator()(const reversed_type_impl& t) { return t.underlying_type()->is_byte_order_equal(); }
    bool operator()(const string_type_impl&) { return true; }
    bool operator()(const bytes_type_impl&) { return true; }
    bool operator()(const timestamp_date_base_class&) { return true; }
    bool operator()(const inet_addr_type_impl&) { return true; }
    bool operator()(const duration_type_impl&) { return true; }
    // FIXME: origin returns false for list.  Why?
    bool operator()(const set_type_impl& s) { return s.get_elements_type()->is_byte_order_equal(); }
};
}

bool abstract_type::is_byte_order_equal() const { return visit(*this, is_byte_order_equal_visitor{}); }

static bool
check_compatibility(const tuple_type_impl &t, const abstract_type& previous, bool (abstract_type::*predicate)(const abstract_type&) const);

static bool
check_compatibility(const vector_type_impl &t, const abstract_type& previous, bool (abstract_type::*predicate)(const abstract_type&) const);

static
bool
is_fixed_size_int_type(const abstract_type& t) {
    using k = abstract_type::kind;
    switch (t.get_kind()) {
    case k::byte:
    case k::short_kind:
    case k::int32:
    case k::long_kind:
        return true;
    case k::ascii:
    case k::boolean:
    case k::bytes:
    case k::counter:
    case k::date:
    case k::decimal:
    case k::double_kind:
    case k::duration:
    case k::empty:
    case k::float_kind:
    case k::inet:
    case k::list:
    case k::map:
    case k::reversed:
    case k::set:
    case k::simple_date:
    case k::time:
    case k::timestamp:
    case k::timeuuid:
    case k::tuple:
    case k::user:
    case k::utf8:
    case k::uuid:
    case k::varint:
    case k::vector:
        return false;
    }
    __builtin_unreachable();
}

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
        bool operator()(const vector_type_impl& t) {
            return check_compatibility(t, previous, &abstract_type::is_compatible_with);
        }
        bool operator()(const collection_type_impl& t) { return is_compatible_with_aux(t, previous); }
        bool operator()(const varint_type_impl& t) {
            return is_fixed_size_int_type(previous);
        }
        bool operator()(const abstract_type& t) { return false; }
    };

    return visit(*this, visitor{previous});
}

cql3::cql3_type abstract_type::as_cql3_type() const {
    return cql3::cql3_type(shared_from_this());
}

static sstring cql3_type_name_impl(const abstract_type& t) {
    struct visitor {
        sstring operator()(const ascii_type_impl&) { return "ascii"; }
        sstring operator()(const boolean_type_impl&) { return "boolean"; }
        sstring operator()(const byte_type_impl&) { return "tinyint"; }
        sstring operator()(const bytes_type_impl&) { return "blob"; }
        sstring operator()(const counter_type_impl&) { return "counter"; }
        sstring operator()(const timestamp_date_base_class&) { return "timestamp"; }
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
            return seastar::format("tuple<{}>", fmt::join(t.all_types() | std::views::transform(std::mem_fn(
                                                                            &abstract_type::as_cql3_type)), ", "));
        }
        sstring operator()(const user_type_impl& u) { return u.get_name_as_cql_string(); }
        sstring operator()(const utf8_type_impl&) { return "text"; }
        sstring operator()(const uuid_type_impl&) { return "uuid"; }
        sstring operator()(const varint_type_impl&) { return "varint"; }
        sstring operator()(const vector_type_impl& v) { return format("vector<{}, {}>", v.get_elements_type()->as_cql3_type(), v.get_dimension()); }
    };
    return visit(t, visitor{});
}

const sstring& abstract_type::cql3_type_name() const {
    if (_cql3_type_name.empty()) {
        auto name = cql3_type_name_impl(*this);
        if (!is_native() && !is_multi_cell() && !is_vector()) {
            name = "frozen<" + name + ">";
        }
        _cql3_type_name = name;
    }
    return _cql3_type_name;
}

sstring abstract_type::cql3_type_name_without_frozen() const {
    return cql3_type_name_impl(*this);
}

void write_collection_value(bytes::iterator& out, data_type type, const data_value& value) {
    if (value.is_null()) {
        auto val_len = -1;
        serialize_int32(out, val_len);
        return;
    }

    size_t val_len = value.serialized_size();

    serialize_int32(out, val_len);

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
    _contains_set_or_map = true;
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
    SCYLLA_ASSERT(!_is_multi_cell);
    auto* p = dynamic_cast<const map_type_impl*>(&previous);
    if (!p) {
        return false;
    }
    return _keys->is_compatible_with(*p->_keys)
            && _values->is_compatible_with(*p->_values);
}

bool
map_type_impl::is_value_compatible_with_frozen(const collection_type_impl& previous) const {
    SCYLLA_ASSERT(!_is_multi_cell);
    auto* p = dynamic_cast<const map_type_impl*>(&previous);
    if (!p) {
        return false;
    }
    return _keys->is_compatible_with(*p->_keys)
            && _values->is_value_compatible_with(*p->_values);
}

std::strong_ordering
map_type_impl::compare_maps(data_type keys, data_type values, managed_bytes_view o1, managed_bytes_view o2) {
    if (o1.empty()) {
        return o2.empty() ? std::strong_ordering::equal : std::strong_ordering::less;
    } else if (o2.empty()) {
        return std::strong_ordering::greater;
    }
    int size1 = read_collection_size(o1);
    int size2 = read_collection_size(o2);
    // FIXME: use std::lexicographical_compare()
    for (int i = 0; i < std::min(size1, size2); ++i) {
        auto k1 = read_collection_key(o1);
        auto k2 = read_collection_key(o2);
        auto cmp = keys->compare(k1, k2);
        if (cmp != 0) {
            return cmp;
        }
        auto v1 = read_collection_value_nonnull(o1);
        auto v2 = read_collection_value_nonnull(o2);
        cmp = values->compare(v1, v2);
        if (cmp != 0) {
            return cmp;
        }
    }
    return size1 <=> size2;
}

static size_t map_serialized_size(const map_type_impl::native_type* m) {
    size_t len = collection_size_len();
    size_t psz = collection_value_len();
    for (auto&& kv : *m) {
        len += psz + kv.first.serialized_size();
        len += psz + kv.second.serialized_size();
    }
    return len;
}

static void
serialize_map(const map_type_impl& t, const void* value, bytes::iterator& out) {
    auto& m = t.from_value(value);
    write_collection_size(out, m.size());
    for (auto&& kv : m) {
        write_collection_value(out, t.get_keys_type(), kv.first);
        write_collection_value(out, t.get_values_type(), kv.second);
    }
}

template <FragmentedView View>
data_value
map_type_impl::deserialize(View in) const {
    native_type m;
    auto size = read_collection_size(in);
    for (int i = 0; i < size; ++i) {
        auto k = _keys->deserialize(read_collection_key(in));
        auto v = _values->deserialize(read_collection_value_nonnull(in));
        m.insert(m.end(), std::make_pair(std::move(k), std::move(v)));
    }
    return make_value(std::move(m));
}
template data_value map_type_impl::deserialize<>(ser::buffer_view<bytes_ostream::fragment_iterator>) const;
template data_value map_type_impl::deserialize<>(managed_bytes_view) const;

template <FragmentedView View>
static void validate_aux(const map_type_impl& t, View v) {
    auto size = read_collection_size(v);
    for (int i = 0; i < size; ++i) {
        t.get_keys_type()->validate(read_collection_key(v));
        t.get_values_type()->validate(read_collection_value_nonnull(v));
    }
}

static sstring map_to_string(const std::vector<std::pair<data_value, data_value>>& v, bool include_frozen_type) {
    std::ostringstream out;

    if (include_frozen_type) {
        out << "(";
    }

    fmt::print(out, "{}", fmt::join(v | std::views::transform([] (const std::pair<data_value, data_value>& p) {
        std::ostringstream out;
        const auto& k = p.first;
        const auto& v = p.second;
        out << "{" << k.type()->to_string_impl(k) << " : ";
        out << v.type()->to_string_impl(v) << "}";
        return std::move(out).str();
    }), ", "));

    if (include_frozen_type) {
        out << ")";
    }

    return std::move(out).str();
}

bytes
map_type_impl::serialize_partially_deserialized_form(
        const std::vector<std::pair<bytes_view, bytes_view>>& v) {
    size_t len = collection_value_len() * v.size() * 2 + collection_size_len();
    for (auto&& e : v) {
        len += e.first.size() + e.second.size();
    }
    bytes b(bytes::initialized_later(), len);
    bytes::iterator out = b.begin();

    write_collection_size(out, v.size());
    for (auto&& e : v) {
        write_collection_value(out, e.first);
        write_collection_value(out, e.second);
    }
    return b;
}

managed_bytes
map_type_impl::serialize_partially_deserialized_form_fragmented(
        const std::vector<std::pair<managed_bytes_view, managed_bytes_view>>& v) {
    size_t len = collection_value_len() * v.size() * 2 + collection_size_len();
    for (auto&& e : v) {
        len += e.first.size() + e.second.size();
    }
    managed_bytes b(managed_bytes::initialized_later(), len);
    managed_bytes_mutable_view out = b;

    write_collection_size(out, v.size());
    for (auto&& e : v) {
        write_collection_value(out, e.first);
        write_collection_value(out, e.second);
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

static void serialize(const abstract_type& t, const void* value, bytes::iterator& out);

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
    : concrete_type(kind::set, make_set_type_name(elements, is_multi_cell), elements, is_multi_cell) {
        _contains_set_or_map = true;
    }

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
    SCYLLA_ASSERT(!_is_multi_cell);
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

template <FragmentedView View>
static void validate_aux(const set_type_impl& t, View v) {
    auto nr = read_collection_size(v);
    for (int i = 0; i != nr; ++i) {
        t.get_elements_type()->validate(read_collection_value_nonnull(v));
    }
}

static size_t listlike_serialized_size(const std::vector<data_value>* s) {
    size_t len = collection_size_len();
    size_t psz = collection_value_len();
    for (auto&& e : *s) {
        len += psz + e.serialized_size();
    }
    return len;
}

static void
serialize_set(const set_type_impl& t, const void* value, bytes::iterator& out) {
    auto& s = t.from_value(value);
    write_collection_size(out, s.size());
    for (auto&& e : s) {
        write_collection_value(out, t.get_elements_type(), e);
    }
}

template <FragmentedView View>
data_value
set_type_impl::deserialize(View in) const {
    auto nr = read_collection_size(in);
    native_type s;
    s.reserve(nr);
    for (int i = 0; i != nr; ++i) {
        auto e = _elements->deserialize(read_collection_value_nonnull(in));
        if (e.is_null()) {
            throw marshal_exception("Cannot deserialize a set");
        }
        s.push_back(std::move(e));
    }
    return make_value(std::move(s));
}
template data_value set_type_impl::deserialize<>(ser::buffer_view<bytes_ostream::fragment_iterator>) const;

bytes
set_type_impl::serialize_partially_deserialized_form(
        const std::vector<bytes_view>& v) {
    return pack(v.begin(), v.end(), v.size());
}

managed_bytes
set_type_impl::serialize_partially_deserialized_form_fragmented(
        const std::vector<managed_bytes_view_opt>& v) {
    return pack_fragmented(v.begin(), v.end(), v.size());
}

template <FragmentedView View>
utils::chunked_vector<managed_bytes_opt> partially_deserialize_listlike(View in) {
    auto nr = read_collection_size(in);
    utils::chunked_vector<managed_bytes_opt> elements;
    elements.reserve(nr);
    for (int i = 0; i != nr; ++i) {
        elements.emplace_back(read_collection_value(in));
    }
    return elements;
}
template utils::chunked_vector<managed_bytes_opt> partially_deserialize_listlike(managed_bytes_view in);
template utils::chunked_vector<managed_bytes_opt> partially_deserialize_listlike(fragmented_temporary_buffer::view in);

template <FragmentedView View>
std::vector<std::pair<managed_bytes, managed_bytes>> partially_deserialize_map(View in) {
    auto nr = read_collection_size(in);
    std::vector<std::pair<managed_bytes, managed_bytes>> elements;
    elements.reserve(nr);
    for (int i = 0; i != nr; ++i) {
        auto key = managed_bytes(read_collection_key(in));
        auto value = managed_bytes_opt(read_collection_value_nonnull(in));
        if (!value) {
            on_internal_error(tlogger, "NULL value in map");
        }
        elements.emplace_back(std::move(key), std::move(*value));
    }
    return elements;
}
template std::vector<std::pair<managed_bytes, managed_bytes>> partially_deserialize_map(managed_bytes_view in);
template std::vector<std::pair<managed_bytes, managed_bytes>> partially_deserialize_map(fragmented_temporary_buffer::view in);

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
    : concrete_type(kind::list, make_list_type_name(elements, is_multi_cell), elements, is_multi_cell) {
        _contains_set_or_map = _elements->contains_set_or_map();
    }

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
    SCYLLA_ASSERT(!_is_multi_cell);
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

template <FragmentedView View>
static void validate_aux(const list_type_impl& t, View v) {
    auto nr = read_collection_size(v);
    for (int i = 0; i != nr; ++i) {
        auto val_opt = read_collection_value(v);
        if (val_opt) {
            t.get_elements_type()->validate(*val_opt);
        }
    }
    if (v.size_bytes()) {
        auto hex = with_linearized(v, [] (bytes_view bv) { return to_hex(bv); });
        throw marshal_exception(format("Validation failed for type {}: bytes remaining after "
                                       "reading all {} elements of the list -> [{}]",
                t.name(), nr, hex));
    }
}

static void
serialize_list(const list_type_impl& t, const void* value, bytes::iterator& out) {
    auto& s = t.from_value(value);
    write_collection_size(out, s.size());
    for (auto&& e : s) {
        write_collection_value(out, t.get_elements_type(), e);
    }
}

template <FragmentedView View>
data_value
list_type_impl::deserialize(View in) const {
    auto nr = read_collection_size(in);
    native_type s;
    s.reserve(nr);
    for (int i = 0; i != nr; ++i) {
        auto serialized_value_opt = read_collection_value(in);
        if (serialized_value_opt) {
            auto e = _elements->deserialize(*serialized_value_opt);
            s.push_back(std::move(e));
        } else {
            s.push_back(data_value::make_null(data_type(shared_from_this())));
        }
    }
    return make_value(std::move(s));
}
template data_value list_type_impl::deserialize<>(ser::buffer_view<bytes_ostream::fragment_iterator>) const;

static sstring vector_to_string(const std::vector<data_value>& v, std::string_view sep) {
    return fmt::to_string(fmt::join(
            v | std::views::transform([] (const data_value& e) { return e.type()->to_string_impl(e); }),
            sep));
}

template <typename F>
static std::optional<data_type> update_listlike(
        const listlike_collection_type_impl& c, F&& f, shared_ptr<const user_type_impl> updated) {
    if (auto e = c.get_elements_type()->update_user_type(updated)) {
        return std::make_optional<data_type>(f(std::move(*e), c.is_multi_cell()));
    }
    return std::nullopt;
}

tuple_type_impl::tuple_type_impl(kind k, sstring name, std::vector<data_type> types, bool freeze_inner)
        : concrete_type(k, std::move(name), { }), _types(std::move(types)) {
    if (freeze_inner) {
        for (auto& t : _types) {
            t = t->freeze();
        }
    }

    set_contains_collections();
}

tuple_type_impl::tuple_type_impl(std::vector<data_type> types, bool freeze_inner)
        : tuple_type_impl{kind::tuple, make_name(types), std::move(types), freeze_inner} {
    set_contains_collections();
}

tuple_type_impl::tuple_type_impl(std::vector<data_type> types)
        : tuple_type_impl(std::move(types), true) {
    set_contains_collections();
}

void tuple_type_impl::set_contains_collections() {
    for (const data_type& t : _types) {
        if (t->contains_set_or_map()) {
            _contains_set_or_map = true;
            break;
        }
    }

    if (_contains_set_or_map) {
        _contains_collection = true;
        return;
    }

    for (const data_type& t : _types) {
        if (t->contains_collection()) {
            _contains_collection = true;
            break;
        }
    }
}

shared_ptr<const tuple_type_impl>
tuple_type_impl::get_instance(std::vector<data_type> types) {
    return intern::get_instance(std::move(types));
}

template <FragmentedView View>
static void validate_aux(const tuple_type_impl& t, View v) {
    auto ti = t.all_types().begin();
    while (ti != t.all_types().end() && v.size_bytes()) {
        std::optional<View> e = read_tuple_element(v);
        if (e) {
            (*ti)->validate(*e);
        }
        ++ti;
    }

    size_t extra_elements = 0;
    while (!v.empty()) {
        read_tuple_element(v);
        extra_elements += 1;
    }

    if (extra_elements > 0) {
        // This function is called for both tuple and user_type, print the name too
        throw marshal_exception(format("Value of type {} contained too many fields (expected {}, got {})",
                                t.name(), t.size(), t.size() + extra_elements));
    }
}

sstring vector_type_impl::make_name(data_type type, size_t dimension) {
    // To keep format compatibility with Origin we never wrap
    // vector name into
    // "org.apache.cassandra.db.marshal.FrozenType(...)".
    return seastar::format("org.apache.cassandra.db.marshal.VectorType({}, {})", type->name(), dimension);
}

vector_type_impl::vector_type_impl(data_type elements, size_t dimension)
        : concrete_type(kind::vector, make_name(elements, dimension),
        elements->value_length_if_fixed() ? std::optional(elements->value_length_if_fixed().value()*dimension):std::nullopt),
        _elements_type(elements), _dimension(dimension) {
    _contains_set_or_map = _elements_type->contains_set_or_map();
}

shared_ptr<const vector_type_impl>
vector_type_impl::get_instance(data_type elements, size_t dimension) {
    return intern::get_instance(elements, dimension);
}

static void serialize_vector(const vector_type_impl& type, const vector_type_impl::native_type* val, bytes::iterator& out) {
    auto elements_type = type.get_elements_type();
    if (type.value_length_if_fixed()) {
        for (const auto& value : *val) {
            value.serialize(out);
        }
    } else {
        for (const auto& value : *val) {
            size_t val_len = value.serialized_size();

            out += unsigned_vint::serialize(val_len, out);
            

            value.serialize(out);
        }
    }
}

std::strong_ordering
vector_type_impl::compare_vectors(data_type elements, size_t dimension, managed_bytes_view o1, managed_bytes_view o2) {
    if (o1.empty()) {
        return o2.empty() ? std::strong_ordering::equal : std::strong_ordering::less;
    } else if (o2.empty()) {
        return std::strong_ordering::greater;
    }

    for (size_t i = 0; i < dimension; i++) {
        auto v1 = read_vector_element(o1, elements->value_length_if_fixed());
        auto v2 = read_vector_element(o2, elements->value_length_if_fixed());
        auto cmp = elements->compare(v1, v2);
        if (cmp != 0) {
            return cmp;
        }
    }

    return std::strong_ordering::equal;
}

template <FragmentedView View>
data_value
deserialize_vector(const vector_type_impl& t, View v){
    vector_type_impl::native_type ret;
    ret.reserve(t.get_dimension());

    auto value_length = t.get_elements_type()->value_length_if_fixed();

    for (size_t i = 0; i < t.get_dimension(); i++) {
        ret.push_back(t.get_elements_type()->deserialize(read_vector_element(v, value_length)));
    }

    return data_value::make(t.shared_from_this(), std::make_unique<vector_type_impl::native_type>(std::move(ret)));
}

static size_t vector_serialized_size(const vector_type_impl::native_type* v) {
    if (v->empty()) {
        return 0;
    }

    auto type = v->front().type();

    size_t len = 0;

    if (type->value_length_if_fixed()) {
        len = v->size() * type->value_length_if_fixed().value();
    } else {
        for (const auto& value : *v) {
            size_t val_len = value.serialized_size();
            len += (size_t)unsigned_vint::serialized_size(val_len) + val_len;
        }
    }
    return len;
}

template <FragmentedView View>
static void validate_aux(const vector_type_impl& t, View v) {
    for (size_t i = 0; i < t.get_dimension(); ++i) {
        auto val = read_vector_element(v, t.get_elements_type()->value_length_if_fixed());
        t.get_elements_type()->validate(val);
    }
    if (v.size_bytes()) {
        auto hex = with_linearized(v, [] (bytes_view bv) { return to_hex(bv); });
        throw marshal_exception(format("Validation failed for type {}: bytes remaining after "
                                       "reading all {} elements of the vector -> [{}]",
                t.name(), t.get_dimension(), hex));
    }
}

static std::optional<data_type> update_user_type_aux(
    const vector_type_impl& m, const shared_ptr<const user_type_impl> updated) {

    auto old_elements = m.get_elements_type();
    if (auto new_elements = old_elements->update_user_type(updated)) {
        return std::make_optional(static_pointer_cast<const abstract_type>(
                vector_type_impl::get_instance(*new_elements, m.get_dimension())));
    }

    return std::nullopt;
}

namespace {
template <FragmentedView View>
struct validate_visitor {
    const View& v;
    ;

    void operator()(const reversed_type_impl& t) {
        visit(*t.underlying_type(), validate_visitor<View>{v});
    }
    void operator()(const counter_type_impl&) {
        visit(*long_type, validate_visitor<View>{v});
    }
    void operator()(const abstract_type&) {}
    template <typename T> void operator()(const integer_type_impl<T>& t) {
        if (v.empty()) {
            return;
        }
        if (v.size_bytes() != sizeof(T)) {
            throw marshal_exception(format("Validation failed for type {}: got {:d} bytes", t.name(), v.size_bytes()));
        }
    }
    void operator()(const byte_type_impl& t) {
        if (v.empty()) {
            return;
        }
        if (v.size_bytes() != 1) {
            throw marshal_exception(format("Expected 1 byte for a tinyint ({:d})", v.size_bytes()));
        }
    }
    void operator()(const short_type_impl& t) {
        if (v.empty()) {
            return;
        }
        if (v.size_bytes() != 2) {
            throw marshal_exception(format("Expected 2 bytes for a smallint ({:d})", v.size_bytes()));
        }
    }
    void operator()(const ascii_type_impl&) {
        // ASCII can be validated independently for each fragment
        for (bytes_view frag : fragment_range(v)) {
            if (!utils::ascii::validate(frag)) {
                throw marshal_exception("Validation failed - non-ASCII character in an ASCII string");
            }
        }
    }
    void operator()(const utf8_type_impl&) {
        auto error_pos = with_simplified(v, [] (FragmentedView auto v) {
            return utils::utf8::validate_with_error_position_fragmented(v);
        });
        if (error_pos) {
            throw marshal_exception(format("Validation failed - non-UTF8 character in a UTF8 string, at byte offset {}", *error_pos));
        }
    }
    void operator()(const bytes_type_impl& t) {}
    void operator()(const boolean_type_impl& t) {
        if (v.empty()) {
            return;
        }
        if (v.size_bytes() != 1) {
            throw marshal_exception(format("Validation failed for boolean, got {:d} bytes", v.size_bytes()));
        }
    }
    void operator()(const timeuuid_type_impl& t) {
        if (v.empty()) {
            return;
        }
        if (v.size_bytes() != 16) {
            throw marshal_exception(format("Validation failed for timeuuid - got {:d} bytes", v.size_bytes()));
        }
        View in = v;
        auto msb = read_simple<uint64_t>(in);
        auto lsb = read_simple<uint64_t>(in);
        utils::UUID uuid(msb, lsb);
        if (uuid.version() != 1) {
            throw marshal_exception(format("Unsupported UUID version ({:d})", uuid.version()));
        }
    }
    void operator()(const timestamp_date_base_class& t) {
        if (v.empty()) {
            return;
        }
        if (v.size_bytes() != sizeof(uint64_t)) {
            throw marshal_exception(format("Validation failed for timestamp - got {:d} bytes", v.size_bytes()));
        }
    }
    void operator()(const duration_type_impl& t) {
        if (v.size_bytes() < 3) {
            throw marshal_exception(format("Expected at least 3 bytes for a duration, got {:d}", v.size_bytes()));
        }

        common_counter_type months, days, nanoseconds;
        std::tie(months, days, nanoseconds) = with_linearized(v, [] (bytes_view bv) {
            return deserialize_counters(bv);
        });

        auto check_counter_range = [] (common_counter_type value, auto counter_value_type_instance,
                                           std::string_view counter_name) {
            using counter_value_type = decltype(counter_value_type_instance);

            if (static_cast<counter_value_type>(value) != value) {
                throw marshal_exception(seastar::format("The duration {} ({:d}) must be a {:d} bit integer", counter_name, value,
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
        if (v.empty()) {
            return;
        }
        if (v.size_bytes() != sizeof(T)) {
            throw marshal_exception(format("Expected {:d} bytes for a floating type, got {:d}", sizeof(T), v.size_bytes()));
        }
    }
    void operator()(const simple_date_type_impl& t) {
        if (v.empty()) {
            return;
        }
        if (v.size_bytes() != 4) {
            throw marshal_exception(format("Expected 4 byte long for date ({:d})", v.size_bytes()));
        }
    }
    void operator()(const time_type_impl& t) {
        if (v.empty()) {
            return;
        }
        if (v.size_bytes() != 8) {
            throw marshal_exception(format("Expected 8 byte long for time ({:d})", v.size_bytes()));
        }
    }
    void operator()(const uuid_type_impl& t) {
        if (v.empty()) {
            return;
        }
        if (v.size_bytes() != 16) {
            throw marshal_exception(format("Validation failed for uuid - got {:d} bytes", v.size_bytes()));
        }
    }
    void operator()(const inet_addr_type_impl& t) {
        if (v.empty()) {
            return;
        }
        if (v.size_bytes() != sizeof(uint32_t) && v.size_bytes() != 16) {
            throw marshal_exception(format("Validation failed for inet_addr - got {:d} bytes", v.size_bytes()));
        }
    }
    void operator()(const map_type_impl& t) {
        with_simplified(v, [&] (FragmentedView auto v) {
            validate_aux(t, v);
        });
    }
    void operator()(const set_type_impl& t) {
        with_simplified(v, [&] (FragmentedView auto v) {
            validate_aux(t, v);
        });
    }
    void operator()(const list_type_impl& t) {
        with_simplified(v, [&] (FragmentedView auto v) {
            validate_aux(t, v);
        });
    }
    void operator()(const tuple_type_impl& t) {
        with_simplified(v, [&] (FragmentedView auto v) {
            validate_aux(t, v);
        });
    }
    void operator()(const vector_type_impl& t) {
        with_simplified(v, [&] (FragmentedView auto v) {
            validate_aux(t, v);
        });
    }
};
}

template <FragmentedView View>
void abstract_type::validate(const View& view) const {
    visit(*this, validate_visitor<View>{view});
}
// Explicit instantiation.
template void abstract_type::validate<>(const single_fragmented_view&) const;
template void abstract_type::validate<>(const fragmented_temporary_buffer::view&) const;
template void abstract_type::validate<>(const managed_bytes_view&) const;

void abstract_type::validate(bytes_view v) const {
    visit(*this, validate_visitor<single_fragmented_view>{single_fragmented_view(v)});
}

static void serialize_aux(const tuple_type_impl& type, const tuple_type_impl::native_type* val, bytes::iterator& out) {
    SCYLLA_ASSERT(val);
    auto& elems = *val;

    SCYLLA_ASSERT(elems.size() <= type.size());

    for (size_t i = 0; i < elems.size(); ++i) {
        const abstract_type& t = type.type(i)->without_reversed();
        const data_value& v = elems[i];
        if (!v.is_null() && t != *v.type()) {
            throw std::runtime_error(format("tuple element type mismatch: expected {}, got {}", t.name(), v.type()->name()));
        }

        if (v.is_null()) {
            write(out, int32_t(-1));
        } else {
            write(out, int32_t(v.serialized_size()));
            v.serialize(out);
        }
    }
}

static size_t concrete_serialized_size(const utils::multiprecision_int& num);

static void serialize_varint_aux(bytes::iterator& out, const boost::multiprecision::cpp_int& num, uint8_t mask) {
    struct inserter_with_prefix {
        bytes::iterator& out;
        uint8_t mask;
        bool first = true;
        inserter_with_prefix& operator*() {
            return *this;
        }
        inserter_with_prefix& operator=(uint8_t value) {
            if (first) {
                if (value & 0x80) {
                    *out++ = 0 ^ mask;
                }
                first = false;
            }
            *out = value ^ mask;
            return *this;
        }
        inserter_with_prefix& operator++() {
            ++out;
            return *this;
        }
    };

    export_bits(num, inserter_with_prefix{out, mask}, 8);
}

static void serialize_varint(bytes::iterator& out, const boost::multiprecision::cpp_int& num) {
    if (num < 0) {
        serialize_varint_aux(out, -num - 1, 0xff);
    } else {
        serialize_varint_aux(out, num, 0);
    }
}

static void serialize_varint(bytes::iterator& out, const utils::multiprecision_int& num) {
    serialize_varint(out, static_cast<const boost::multiprecision::cpp_int&>(num));
}

static void serialize(const abstract_type& t, const void* value, bytes::iterator& out);

namespace {
struct serialize_visitor {
    bytes::iterator& out;
    ;
    void operator()(const reversed_type_impl& t, const void* v) { return serialize(*t.underlying_type(), v, out); }
    void operator()(const counter_type_impl& t, const void* v) { return serialize(*long_type, v, out); }
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
    void operator()(const timestamp_date_base_class& t, const timestamp_date_base_class::native_type* v1) {
        if (v1->empty()) {
            return;
        }
        uint64_t v = v1->get().time_since_epoch().count();
        v = net::hton(v);
        out = std::copy_n(reinterpret_cast<const char*>(&v), sizeof(v), out);
    }
    void operator()(const timeuuid_type_impl& t, const timeuuid_type_impl::native_type* uuid1) {
        if (uuid1->empty()) {
            return;
        }
        auto uuid = uuid1->get();
        uuid.serialize(out);
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
        if (value->empty()) {
            return;
        }

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
        if (value->empty()) {
            return;
        }

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

        serialize_varint(out, num1->get());
    }
    void operator()(const decimal_type_impl& t, const decimal_type_impl::native_type* bd1) {
        if (bd1->empty()) {
            return;
        }
        auto&& bd = std::move(*bd1).get();
        auto u = net::hton(bd.scale());
        out = std::copy_n(reinterpret_cast<const char*>(&u), sizeof(int32_t), out);
        serialize_varint(out, bd.unscaled_value());
    }
    void operator()(const duration_type_impl& t, const duration_type_impl::native_type* m) {
        if (m->empty()) {
            return;
        }
        const auto& d = m->get();
        out += signed_vint::serialize(d.months, out);
        out += signed_vint::serialize(d.days, out);
        out += signed_vint::serialize(d.nanoseconds, out);
    }
    void operator()(const list_type_impl& t, const void* value) {
        serialize_list(t, value, out);
    }
    void operator()(const map_type_impl& t, const void* value) {
        serialize_map(t, value, out);
    }
    void operator()(const set_type_impl& t, const void* value) {
        serialize_set(t, value, out);
    }
    void operator()(const tuple_type_impl& t, const tuple_type_impl::native_type* value) {
        return serialize_aux(t, value, out);
    }
    void operator()(const vector_type_impl& t, const vector_type_impl::native_type* value) {
        serialize_vector(t, value, out);
    }
};
}

static void serialize(const abstract_type& t, const void* value, bytes::iterator& out) {
    visit(t, value, serialize_visitor{out});
}

template <FragmentedView View>
data_value collection_type_impl::deserialize_impl(View v) const {
    struct visitor {
        View v;
        ;
        data_value operator()(const abstract_type&) {
            on_internal_error(tlogger, "collection_type_impl::deserialize called on a non-collection type. This should be impossible.");
        }
        data_value operator()(const list_type_impl& t) {
            return t.deserialize(v);
        }
        data_value operator()(const map_type_impl& t) {
            return t.deserialize(v);
        }
        data_value operator()(const set_type_impl& t) {
            return t.deserialize(v);
        }
    };
    return ::visit(*this, visitor{v});
}
// Explicit instantiation.
// This should be repeated for every View type passed to collection_type_impl::deserialize.
template data_value collection_type_impl::deserialize_impl<>(ser::buffer_view<bytes_ostream::fragment_iterator>) const;
template data_value collection_type_impl::deserialize_impl<>(fragmented_temporary_buffer::view) const;
template data_value collection_type_impl::deserialize_impl<>(single_fragmented_view) const;
template data_value collection_type_impl::deserialize_impl<>(managed_bytes_view) const;

template int read_collection_size(ser::buffer_view<bytes_ostream::fragment_iterator>& in);
template ser::buffer_view<bytes_ostream::fragment_iterator> read_collection_value_nonnull(ser::buffer_view<bytes_ostream::fragment_iterator>& in);

template <FragmentedView View>
data_value deserialize_aux(const tuple_type_impl& t, View v) {
    tuple_type_impl::native_type ret;
    ret.reserve(t.all_types().size());
    auto ti = t.all_types().begin();
    while (ti != t.all_types().end() && v.size_bytes()) {
        data_value obj = data_value::make_null(*ti);
        std::optional<View> e = read_tuple_element(v);
        if (e) {
            obj = (*ti)->deserialize(*e);
        }
        ret.push_back(std::move(obj));
        ++ti;
    }
    while (ti != t.all_types().end()) {
        ret.push_back(data_value::make_null(*ti++));
    }
    return data_value::make(t.shared_from_this(), std::make_unique<tuple_type_impl::native_type>(std::move(ret)));
}

template <FragmentedView View>
std::optional<std::optional<View>> read_nth_tuple_element(View serialized_tuple, std::size_t element_index) {
    for (std::size_t i = 0; serialized_tuple.size_bytes() > 0; i++) {
        // element being std::nullopt means that its value is NULL.
        std::optional<View> element = read_tuple_element(serialized_tuple);

        if (i == element_index) {
            return std::make_optional(element);
        }
    }

    return std::nullopt;
}

template std::optional<std::optional<managed_bytes_view>> read_nth_tuple_element(managed_bytes_view serialized_tuple,
                                                                              std::size_t element_index);
template std::optional<std::optional<fragmented_temporary_buffer::view>> read_nth_tuple_element(
    fragmented_temporary_buffer::view serialized_tuple,
    std::size_t element_index);

template <FragmentedView View>
std::optional<View> read_nth_user_type_field(View serialized_user_type, std::size_t element_index) {
    // UDT is serialized as a tuple of field values, so we can use read_nth_tuple_element to read
    // the value of nth UDT field.
    std::optional<std::optional<View>> read_field = read_nth_tuple_element(serialized_user_type, element_index);

    if (!read_field.has_value()) {
        // There is no field with this index, assume that this field is NULL.
        return std::nullopt;
    }

    // Field value found, return it.
    return *read_field;
}

template std::optional<managed_bytes_view> read_nth_user_type_field(managed_bytes_view serialized_user_type,
                                                                    std::size_t element_index);
template std::optional<fragmented_temporary_buffer::view> read_nth_user_type_field(
    fragmented_temporary_buffer::view serialized_user_type,
    std::size_t element_index);

template<FragmentedView View>
utils::multiprecision_int deserialize_value(const varint_type_impl&, View v) {
    if (v.empty()) {
        throw marshal_exception("cannot deserialize multiprecision int - empty buffer");
    }
    skip_empty_fragments(v);
    bool negative = v.current_fragment().front() < 0;
    utils::multiprecision_int num;
  while (v.size_bytes()) {
    for (uint8_t b : v.current_fragment()) {
        if (negative) {
            b = ~b;
        }
        num <<= 8;
        num += b;
    }
    v.remove_current();
  }
    if (negative) {
        num += 1;
    }
    return negative ? -num : num;
}

template<typename T, FragmentedView View>
T deserialize_value(const floating_type_impl<T>&, View v) {
    typename int_of_size<T>::itype i = read_simple<typename int_of_size<T>::itype>(v);
    if (v.size_bytes()) {
        throw marshal_exception(format("cannot deserialize floating - {:d} bytes left", v.size_bytes()));
    }
    T d;
    memcpy(&d, &i, sizeof(T));
    return d;
}

template<FragmentedView View>
big_decimal deserialize_value(const decimal_type_impl&, View v) {
    auto scale = read_simple<int32_t>(v);
    auto unscaled = deserialize_value(static_cast<const varint_type_impl&>(*varint_type), v);
    return big_decimal(scale, unscaled);
}

template<FragmentedView View>
cql_duration deserialize_value(const duration_type_impl& t, View v) {
    common_counter_type months, days, nanoseconds;
    std::tie(months, days, nanoseconds) = with_linearized(v, [] (bytes_view bv) {
        return deserialize_counters(bv);
    });
    return cql_duration(months_counter(months), days_counter(days), nanoseconds_counter(nanoseconds));
}

template<FragmentedView View>
inet_address deserialize_value(const inet_addr_type_impl&, View v) {
    switch (v.size_bytes()) {
    case 4:
        // gah. read_simple_be, please...
        return inet_address(::in_addr{net::hton(read_simple<uint32_t>(v))});
    case 16:;
        ::in6_addr buf;
        read_fragmented(v, sizeof(buf), reinterpret_cast<bytes::value_type*>(&buf));
        return inet_address(buf);
    default:
        throw marshal_exception(format("cannot deserialize inet_address, unsupported size {:d} bytes", v.size_bytes()));
    }
}

template<FragmentedView View>
utils::UUID deserialize_value(const uuid_type_impl&, View v) {
    auto msb = read_simple<uint64_t>(v);
    auto lsb = read_simple<uint64_t>(v);
    if (v.size_bytes()) {
        throw marshal_exception(format("cannot deserialize uuid, {:d} bytes left", v.size_bytes()));
    }
    return utils::UUID(msb, lsb);
}

template<FragmentedView View>
utils::UUID deserialize_value(const timeuuid_type_impl&, View v) {
    return deserialize_value(static_cast<const uuid_type_impl&>(*uuid_type), v);
}

template<FragmentedView View>
db_clock::time_point deserialize_value(const timestamp_date_base_class&, View v) {
    auto v2 = read_simple_exactly<uint64_t>(v);
    return db_clock::time_point(db_clock::duration(v2));
}

template<FragmentedView View>
uint32_t deserialize_value(const simple_date_type_impl&, View v) {
    return read_simple_exactly<uint32_t>(v);
}

template<FragmentedView View>
int64_t deserialize_value(const time_type_impl&, View v) {
    return read_simple_exactly<int64_t>(v);
}

template<FragmentedView View>
bool deserialize_value(const boolean_type_impl&, View v) {
    if (v.size_bytes() != 1) {
        throw marshal_exception(format("cannot deserialize boolean, size mismatch ({:d})", v.size_bytes()));
    }
    skip_empty_fragments(v);
    return v.current_fragment().front() != 0;
}

template<typename T, FragmentedView View>
T deserialize_value(const integer_type_impl<T>& t, View v) {
    return read_simple_exactly<T>(v);
}

template<FragmentedView View>
sstring deserialize_value(const string_type_impl&, View v) {
    // FIXME: validation?
    sstring buf(sstring::initialized_later(), v.size_bytes());
    auto out = buf.begin();
    while (v.size_bytes()) {
        out = std::copy(v.current_fragment().begin(), v.current_fragment().end(), out);
        v.remove_current();
    }
    return buf;
}

template<typename T>
decltype(auto) deserialize_value(const T& t, bytes_view v) {
    return deserialize_value(t, single_fragmented_view(v));
}

namespace {
template <FragmentedView View>
struct deserialize_visitor {
    View v;
    data_value operator()(const reversed_type_impl& t) { return t.underlying_type()->deserialize(v); }
    template <typename T> data_value operator()(const T& t) {
        if (!v.size_bytes()) {
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
        return t.make_value(std::make_unique<bytes_type_impl::native_type>(linearized(v)));
    }
    data_value operator()(const counter_type_impl& t) {
        return static_cast<const long_type_impl&>(*long_type).make_value(read_simple_exactly<int64_t>(v));
    }
    data_value operator()(const list_type_impl& t) {
        return t.deserialize(v);
    }
    data_value operator()(const map_type_impl& t) {
        return t.deserialize(v);
    }
    data_value operator()(const set_type_impl& t) {
        return t.deserialize(v);
    }
    data_value operator()(const tuple_type_impl& t) { return deserialize_aux(t, v); }
    data_value operator()(const vector_type_impl& t) { return deserialize_vector(t,v); }
    data_value operator()(const user_type_impl& t) { return deserialize_aux(t, v); }
    data_value operator()(const empty_type_impl& t) { return data_value(empty_type_representation()); }
};
}

template <FragmentedView View>
data_value abstract_type::deserialize_impl(View v) const {
    return visit(*this, deserialize_visitor<View>{v});
}
// Explicit instantiation.
// This should be repeated for every type passed to deserialize().
template data_value abstract_type::deserialize_impl<>(fragmented_temporary_buffer::view) const;
template data_value abstract_type::deserialize_impl<>(single_fragmented_view) const;
template data_value abstract_type::deserialize_impl<>(ser::buffer_view<bytes_ostream::fragment_iterator>) const;
template data_value abstract_type::deserialize_impl<>(managed_bytes_view) const;

std::strong_ordering compare_aux(const tuple_type_impl& t, const managed_bytes_view& v1, const managed_bytes_view& v2) {
    // This is a slight modification of lexicographical_tri_compare:
    // when the only difference between the tuples is that one of them has additional trailing nulls,
    // we consider them equal. For example, in the following CQL scenario:
    // 1. create type ut (a int);
    // 2. create table cf (a int primary key, b frozen<ut>);
    // 3. insert into cf (a, b) values (0, (0));
    // 4. alter type ut add b int;
    // 5. select * from cf where b = {a:0,b:null};
    // the row with a = 0 should be returned, even though the value stored in the database is shorter
    // (by one null) than the value given by the user.

    auto types_first = t.all_types().begin();
    auto types_last = t.all_types().end();

    auto first1 = tuple_deserializing_iterator::start(v1);
    auto last1 = tuple_deserializing_iterator::finish(v1);

    auto first2 = tuple_deserializing_iterator::start(v2);
    auto last2 = tuple_deserializing_iterator::finish(v2);

    while (types_first != types_last && first1 != last1 && first2 != last2) {
        if (auto c = tri_compare_opt(*types_first, *first1, *first2); c != 0) {
            return c;
        }

        ++first1;
        ++first2;
        ++types_first;
    }

    while (types_first != types_last && first1 != last1) {
        if (*first1) {
            return std::strong_ordering::greater;
        }

        ++first1;
        ++types_first;
    }

    while (types_first != types_last && first2 != last2) {
        if (*first2) {
            return std::strong_ordering::less;
        }

        ++first2;
        ++types_first;
    }

    return std::strong_ordering::equal;
}

namespace {

struct compare_visitor {
    managed_bytes_view v1;
    managed_bytes_view v2;

    template <std::invocable<> Func>
    requires std::same_as<std::strong_ordering, std::invoke_result_t<Func>>
    std::strong_ordering with_empty_checks(Func func) {
        if (v1.empty()) {
            return v2.empty() ? std::strong_ordering::equal : std::strong_ordering::less;
        }
        if (v2.empty()) {
            return std::strong_ordering::greater;
        }
        return func();
    }

    template <typename T> std::strong_ordering operator()(const simple_type_impl<T>&) {
      return with_empty_checks([&] {
        T a = simple_type_traits<T>::read_nonempty(v1);
        T b = simple_type_traits<T>::read_nonempty(v2);
        return a <=> b;
      });
    }
    std::strong_ordering operator()(const string_type_impl&) { return compare_unsigned(v1, v2); }
    std::strong_ordering operator()(const bytes_type_impl&) { return compare_unsigned(v1, v2); }
    std::strong_ordering operator()(const duration_type_impl&) { return compare_unsigned(v1, v2); }
    std::strong_ordering operator()(const inet_addr_type_impl&) { return compare_unsigned(v1, v2); }
    std::strong_ordering operator()(const date_type_impl&) {
        // This is not the same behaviour as timestamp_type_impl
        return compare_unsigned(v1, v2);
    }
    std::strong_ordering operator()(const timeuuid_type_impl&) {
      return with_empty_checks([&] {
        return with_linearized(v1, [&] (bytes_view v1) {
            return with_linearized(v2, [&] (bytes_view v2) {
                return utils::timeuuid_tri_compare(v1, v2);
            });
        });
      });
    }
    std::strong_ordering operator()(const listlike_collection_type_impl& l) {
        using llpdi = listlike_partial_deserializing_iterator;
        return with_empty_checks([&] {
            return std::lexicographical_compare_three_way(llpdi::begin(v1), llpdi::end(v1), llpdi::begin(v2),
                    llpdi::end(v2),
                    [&] (const managed_bytes_view_opt& o1, const managed_bytes_view_opt& o2) {
                        if (!o1.has_value() || !o2.has_value()) {
                            return o1.has_value() <=> o2.has_value();
                        } else {
                            return l.get_elements_type()->compare(*o1, *o2);
                        }
            });
        });
    }
    std::strong_ordering operator()(const map_type_impl& m) {
        return map_type_impl::compare_maps(m.get_keys_type(), m.get_values_type(), v1, v2);
    }
    std::strong_ordering operator()(const uuid_type_impl&) {
        if (v1.size() < 16) {
            return v2.size() < 16 ? std::strong_ordering::equal : std::strong_ordering::less;
        }
        if (v2.size() < 16) {

            return std::strong_ordering::greater;
        }
        auto c1 = (v1[6] >> 4) & 0x0f;
        auto c2 = (v2[6] >> 4) & 0x0f;

        if (c1 != c2) {
            return c1 <=> c2;
        }

        if (c1 == 1) {
            return with_linearized(v1, [&] (bytes_view v1) {
                return with_linearized(v2, [&] (bytes_view v2) {
                    return utils::uuid_tri_compare_timeuuid(v1, v2);
                });
            });
        }
        return compare_unsigned(v1, v2);
    }
    std::strong_ordering operator()(const empty_type_impl&) { return std::strong_ordering::equal; }
    std::strong_ordering operator()(const tuple_type_impl& t) { return compare_aux(t, v1, v2); }
    std::strong_ordering operator()(const vector_type_impl& t) {
        return vector_type_impl::compare_vectors(t.get_elements_type(),t.get_dimension(), v1, v2); }
    std::strong_ordering operator()(const counter_type_impl&) {
        // untouched (empty) counter evaluates as 0
        const auto a = v1.empty() ? 0 : simple_type_traits<int64_t>::read_nonempty(v1);
        const auto b = v2.empty() ? 0 : simple_type_traits<int64_t>::read_nonempty(v2);
        return a <=> b;
    }
    std::strong_ordering operator()(const decimal_type_impl& d) {
      return with_empty_checks([&] {
        auto a = deserialize_value(d, v1);
        auto b = deserialize_value(d, v2);
        return a <=> b;
      });
    }
    std::strong_ordering operator()(const varint_type_impl& v) {
      return with_empty_checks([&] {
        auto a = deserialize_value(v, v1);
        auto b = deserialize_value(v, v2);
        return a == b ? std::strong_ordering::equal : a < b ? std::strong_ordering::less : std::strong_ordering::greater;
      });
    }
    template <typename T> std::strong_ordering operator()(const floating_type_impl<T>&) {
      return with_empty_checks([&] {
        T a = simple_type_traits<T>::read_nonempty(v1);
        T b = simple_type_traits<T>::read_nonempty(v2);

        // in java world NaN == NaN and NaN is greater than anything else
        if (std::isnan(a) && std::isnan(b)) {
            return std::strong_ordering::equal;
        } else if (std::isnan(a)) {
            return std::strong_ordering::greater;
        } else if (std::isnan(b)) {
            return std::strong_ordering::less;
        }
        // also -0 < 0
        if (std::signbit(a) && !std::signbit(b)) {
            return std::strong_ordering::less;
        } else if (!std::signbit(a) && std::signbit(b)) {
            return std::strong_ordering::greater;
        }
        // note: float <=> returns std::partial_ordering
        return a == b ? std::strong_ordering::equal : a < b ? std::strong_ordering::less : std::strong_ordering::greater;
      });
    }
    std::strong_ordering operator()(const reversed_type_impl& r) { return r.underlying_type()->compare(v2, v1); }
};
}

std::strong_ordering abstract_type::compare(bytes_view v1, bytes_view v2) const {
    return compare(managed_bytes_view(v1), managed_bytes_view(v2));
}

std::strong_ordering abstract_type::compare(managed_bytes_view v1, managed_bytes_view v2) const {
    try {
        return visit(*this, compare_visitor{v1, v2});
    } catch (const marshal_exception&) {
        on_types_internal_error(std::current_exception());
    }
}

std::strong_ordering abstract_type::compare(managed_bytes_view v1, bytes_view v2) const {
    return compare(v1, managed_bytes_view(v2));
}

std::strong_ordering abstract_type::compare(bytes_view v1, managed_bytes_view v2) const {
    return compare(managed_bytes_view(v1), v2);
}

bool abstract_type::equal(bytes_view v1, bytes_view v2) const {
    return ::visit(*this, [&](const auto& t) {
        if (is_byte_order_equal_visitor{}(t)) {
            return compare_unsigned(v1, v2) == 0;
        }
        return compare_visitor{v1, v2}(t) == 0;
    });
}

bool abstract_type::equal(managed_bytes_view v1, managed_bytes_view v2) const {
    return ::visit(*this, [&](const auto& t) {
        if (is_byte_order_equal_visitor{}(t)) {
            return compare_unsigned(v1, v2) == 0;
        }
        return compare_visitor{v1, v2}(t) == 0;
    });
}

bool abstract_type::equal(managed_bytes_view v1, bytes_view v2) const {
    return equal(v1, managed_bytes_view(v2));
}

bool abstract_type::equal(bytes_view v1, managed_bytes_view v2) const {
    return equal(managed_bytes_view(v1), v2);
}

// Count number of ':' which are not preceded by '\'.
static std::size_t count_segments(std::string_view v) {
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
static std::vector<std::string_view> split_field_strings(std::string_view v) {
    if (v.empty()) {
        return std::vector<std::string_view>();
    }
    std::vector<std::string_view> result;
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
static std::string unescape(std::string_view s) {
    return boost::regex_replace(std::string(s), boost::regex("\\\\([@:])"), "$1");
}

// Replace ":" with "\:" and "@" with "\@".
static std::string escape(std::string_view s) {
    return boost::regex_replace(std::string(s), boost::regex("[@:]"), "\\\\$0");
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
    return hash(managed_bytes_view(v));
}

size_t abstract_type::hash(managed_bytes_view v) const {
    struct visitor {
        managed_bytes_view v;
        size_t operator()(const reversed_type_impl& t) { return t.underlying_type()->hash(v); }
        size_t operator()(const counter_type_impl&) { return long_type->hash(v); }
        size_t operator()(const abstract_type& t) { return std::hash<managed_bytes_view>()(v); }
        size_t operator()(const tuple_type_impl& t) {
            auto apply_hash = [] (auto&& type_value) {
                auto&& type = std::get<0>(type_value);
                auto&& value = std::get<1>(type_value);
                return value ? type->hash(*value) : 0;
            };
            // FIXME: better accumulation function
            return std::ranges::fold_left(std::views::zip(t.all_types(), t.make_range(v)) | std::views::transform(apply_hash),
                    0, std::bit_xor<>());
        }
        size_t operator()(const varint_type_impl& t) {
            return std::hash<sstring>()(with_linearized(v, [&] (bytes_view bv) { return t.to_string(bv); }));
        }
        size_t operator()(const decimal_type_impl& t) {
            return std::hash<sstring>()(with_linearized(v, [&] (bytes_view bv) { return t.to_string(bv); }));
        }
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
static size_t concrete_serialized_size(const timestamp_date_base_class::native_type&) { return 8; }
static size_t concrete_serialized_size(const timeuuid_type_impl::native_type&) { return 16; }
static size_t concrete_serialized_size(const simple_date_type_impl::native_type&) { return 4; }
static size_t concrete_serialized_size(const string_type_impl::native_type& v) { return v.size(); }
static size_t concrete_serialized_size(const bytes_type_impl::native_type& v) { return v.size(); }
static size_t concrete_serialized_size(const inet_addr_type_impl::native_type& v) { return v.get().size(); }

static size_t concrete_serialized_size_aux(const boost::multiprecision::cpp_int& num) {
    if (num) {
        return align_up<size_t>(boost::multiprecision::msb(num) + 2, 8u) / 8;
    } else {
        return 1;
    }
}

static size_t concrete_serialized_size(const boost::multiprecision::cpp_int& num) {
    if (num < 0) {
        return concrete_serialized_size_aux(-num - 1);
    }
    return concrete_serialized_size_aux(num);
}

static size_t concrete_serialized_size(const utils::multiprecision_int& num) {
    return concrete_serialized_size(static_cast<const boost::multiprecision::cpp_int&>(num));
}

static size_t concrete_serialized_size(const varint_type_impl::native_type& v) {
    return concrete_serialized_size(v.get());
}

static size_t concrete_serialized_size(const decimal_type_impl::native_type& v) {
    const boost::multiprecision::cpp_int& uv = v.get().unscaled_value();
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
    size_t operator()(const counter_type_impl&, const void* v) { return serialized_size(*long_type, v); }
    size_t operator()(const empty_type_impl&, const void*) { return 0; }
    template <typename T>
    size_t operator()(const concrete_type<T>& t, const typename concrete_type<T>::native_type* v) {
        if (v->empty()) {
            return 0;
        }
        return concrete_serialized_size(*v);
    }
    size_t operator()(const map_type_impl& t, const map_type_impl::native_type* v) { return map_serialized_size(v); }
    size_t operator()(const concrete_type<std::vector<data_value>, listlike_collection_type_impl>& t,
            const std::vector<data_value>* v) {
        return listlike_serialized_size(v);
    }
    size_t operator()(const vector_type_impl& t, const vector_type_impl::native_type* v) { return vector_serialized_size(v); }
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

seastar::net::inet_address inet_addr_type_impl::from_string_view(std::string_view s) {
    try {
        return inet_address(std::string(s.data(), s.size()));
    } catch (...) {
        throw marshal_exception(seastar::format("Failed to parse inet_addr from '{}'", s));
    }
}

utils::UUID uuid_type_impl::from_string_view(std::string_view s) {
    static const boost::regex re("^[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12}$");
    if (!boost::regex_match(s.begin(), s.end(), re)) {
        throw marshal_exception(seastar::format("Cannot parse uuid from '{}'", s));
    }
    return utils::UUID(s);
}

utils::UUID timeuuid_type_impl::from_string_view(std::string_view s) {
    static const boost::regex re("^[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12}$");
    if (!boost::regex_match(s.begin(), s.end(), re)) {
        throw marshal_exception(seastar::format("Invalid UUID format ({})", s));
    }
    utils::UUID v(s);
    if (v.version() != 1) {
        throw marshal_exception(format("Unsupported UUID version ({:d})", v.version()));
    }
    return v;
}

namespace {
struct from_string_visitor {
    std::string_view s;
    bytes operator()(const reversed_type_impl& r) { return r.underlying_type()->from_string(s); }
    bytes operator()(const counter_type_impl&) { return long_type->from_string(s); }
    template <typename T> bytes operator()(const integer_type_impl<T>& t) { return decompose_value(parse_int(t, s)); }
    bytes operator()(const ascii_type_impl&) {
        auto bv = bytes_view(reinterpret_cast<const int8_t*>(s.begin()), s.size());
        if (utils::ascii::validate(bv)) {
            return to_bytes(bv);
        } else {
            throw marshal_exception(seastar::format("Invalid ASCII character in string literal: '{}'", s));
        }
    }
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
            throw marshal_exception(seastar::format("unable to make boolean from '{}'", s));
        }
        return serialize_value(t, v);
    }
    bytes operator()(const timeuuid_type_impl&) {
        if (s.empty()) {
            return bytes();
        }
        return timeuuid_type_impl::from_string_view(s).serialize();
    }
    bytes operator()(const timestamp_date_base_class& t) {
        if (s.empty()) {
            return bytes();
        }
        return serialize_value(t, timestamp_type_impl::from_string_view(s));
    }
    bytes operator()(const simple_date_type_impl& t) {
        if (s.empty()) {
            return bytes();
        }
        return serialize_value(t, simple_date_type_impl::from_string_view(s));
    }
    bytes operator()(const time_type_impl& t) {
        if (s.empty()) {
            return bytes();
        }
        return serialize_value(t, time_type_impl::from_string_view(s));
    }
    bytes operator()(const uuid_type_impl&) {
        if (s.empty()) {
            return bytes();
        }
        return uuid_type_impl::from_string_view(s).serialize();
    }
    template <typename T> bytes operator()(const floating_type_impl<T>& t) {
        if (s.empty()) {
            return bytes();
        }
        try {
            auto d = boost::lexical_cast<T>(s.begin(), s.size());
            return serialize_value(t, d);
        } catch (const boost::bad_lexical_cast& e) {
            throw marshal_exception(seastar::format("Invalid number format '{}'", s));
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
            throw marshal_exception(seastar::format("unable to make int from '{}'", s));
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
            throw marshal_exception(seastar::format("unable to make BigDecimal from '{}'", s));
        }
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
        return serialize_value(t, t.from_string_view(s));
    }
    bytes operator()(const tuple_type_impl& t) {
        std::vector<std::string_view> field_strings = split_field_strings(s);
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
        return concat_fields(fields, field_len);
    }
    bytes operator()(const vector_type_impl& t) {
        // FIXME:
        abort();
        return bytes();
    }
    bytes operator()(const collection_type_impl&) {
        // FIXME:
        abort();
        return bytes();
    }
};
}

bytes abstract_type::from_string(std::string_view s) const { return visit(*this, from_string_visitor{s}); }

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

    return std::move(out).str();
}

template <typename N, typename A, typename F>
static sstring format_if_not_empty(
        const concrete_type<N, A>& type, const typename concrete_type<N, A>::native_type* b, F&& f) {
    if (b->empty()) {
        return {};
    }
    return f(static_cast<const N&>(*b));
}

sstring timestamp_to_json_string(const timestamp_date_base_class& t, const bytes_view& bv)
{
    auto tp = value_cast<const timestamp_date_base_class::native_type>(t.deserialize(bv));
    return format_if_not_empty(t, &tp, [](const db_clock::time_point& v) { return time_point_to_string(v, false); });
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
        return format_if_not_empty(b, v, [] (const bool b) { return fmt::to_string(b); });
    }
    sstring operator()(const timestamp_date_base_class& d, const timestamp_date_base_class::native_type* v) {
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
        return format_if_not_empty(a, v, [] (const seastar::net::inet_address& addr) { return fmt::to_string(addr); });
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
    sstring operator()(const counter_type_impl& c, const void* v) { return to_string_impl(*long_type, v); }
    sstring operator()(const simple_date_type_impl& s, const simple_date_type_impl::native_type* v) {
        return format_if_not_empty(s, v, simple_date_to_string);
    }
    sstring operator()(const string_type_impl& s, const string_type_impl::native_type* v) {
        return format_if_not_empty(s, v, [] (const sstring& s) { return s; });
    }
    sstring operator()(const time_type_impl& t, const time_type_impl::native_type* v) {
        return format_if_not_empty(t, v, time_to_string);
    }
    sstring operator()(const timeuuid_type_impl& t, const timeuuid_type_impl::native_type* v) {
        return format_if_not_empty(t, v, [] (const utils::UUID& v) { return fmt::to_string(v); });
    }
    sstring operator()(const tuple_type_impl& t, const tuple_type_impl::native_type* v) {
        return format_if_not_empty(t, v, [&t] (const tuple_type_impl::native_type& b) { return tuple_to_string(t, b); });
    }
    sstring operator()(const vector_type_impl& vt, const vector_type_impl::native_type* v) {
        return format_if_not_empty(
                vt, v, [] (const vector_type_impl::native_type& v) { return vector_to_string(v, ", "); });
    }
    sstring operator()(const uuid_type_impl& u, const uuid_type_impl::native_type* v) {
        return format_if_not_empty(u, v, [] (const utils::UUID& v) { return fmt::to_string(v); });
    }
    sstring operator()(const varint_type_impl& t, const varint_type_impl::native_type* v) {
        return format_if_not_empty(t, v, [] (const utils::multiprecision_int& v) { return v.str(); });
    }
};
}

static sstring to_string_impl(const abstract_type& t, const void* v) {
    return visit(t, v, to_string_impl_visitor{});
}

sstring abstract_type::to_string_impl(const data_value& v) const {
    return ::to_string_impl(*this, get_value_ptr(v));
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

static bool
check_compatibility(const vector_type_impl &t, const abstract_type& previous, bool (abstract_type::*predicate)(const abstract_type&) const) {
    auto* x = dynamic_cast<const vector_type_impl*>(&previous);
    if (!x) {
        return false;
    }
    return ((*t.get_elements_type()).*predicate)(*x->get_elements_type()) && t.get_dimension() == x->get_dimension();
}

static bool is_value_compatible_with_internal_aux(const user_type_impl& t, const abstract_type& previous) {
    if (&t == &previous) {
        return true;
    }

    if (!previous.is_user_type()) {
        return false;
    }

    auto& x = static_cast<const user_type_impl&>(previous);

    if (t.is_multi_cell() != x.is_multi_cell() || t._keyspace != x._keyspace) {
        return false;
    }

    auto c = std::mismatch(
            t.all_types().begin(), t.all_types().end(),
            x.all_types().begin(), x.all_types().end(),
            [] (const data_type& a, const data_type& b) { return a->is_compatible_with(*b); });
    return c.second == x.all_types().end(); // `this` allowed to have additional fields
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
        bool operator()(const timestamp_date_base_class& t) { return is_date_long_or_timestamp(other); }
        bool operator()(const uuid_type_impl&) {
            return other.get_kind() == abstract_type::kind::uuid || other.get_kind() == abstract_type::kind::timeuuid;
        }
        bool operator()(const varint_type_impl& t) {
            return other == t || int32_type->is_value_compatible_with(other) ||
                   long_type->is_value_compatible_with(other) ||
                   short_type->is_value_compatible_with(other) ||
                   byte_type->is_value_compatible_with(other);
        }
        bool operator()(const user_type_impl& t) { return is_value_compatible_with_internal_aux(t, other); }
        bool operator()(const tuple_type_impl& t) {
            return check_compatibility(t, other, &abstract_type::is_value_compatible_with);
        }
        bool operator()(const vector_type_impl& t) {
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

std::optional<size_t>
user_type_impl::idx_of_field(const bytes_view& name) const {
    for (size_t i = 0; i < _field_names.size(); ++i) {
        if (name == bytes_view(_field_names[i])) {
            return {i};
        }
    }
    return {};
}

shared_ptr<const user_type_impl>
user_type_impl::get_instance(sstring keyspace, bytes name,
        std::vector<bytes> field_names, std::vector<data_type> field_types, bool multi_cell) {
    return intern::get_instance(std::move(keyspace), std::move(name), std::move(field_names), std::move(field_types), multi_cell);
}

sstring
tuple_type_impl::make_name(const std::vector<data_type>& types) {
    // To keep format compatibility with Origin we never wrap
    // tuple name into
    // "org.apache.cassandra.db.marshal.FrozenType(...)".
    // Even when the tuple is frozen.
    // For more details see #4087
    return seastar::format("org.apache.cassandra.db.marshal.TupleType({})", fmt::join(types | std::views::transform(std::mem_fn(&abstract_type::name)), ", "));
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
    void* operator()(const counter_type_impl& t) {
        return visit(*long_type, native_value_clone_visitor{from});
    }
    template <typename N, typename A> void* operator()(const concrete_type<N, A>&) {
        using nt = typename concrete_type<N, A>::native_type;
        return new nt(*reinterpret_cast<const nt*>(from));
    }
    void* operator()(const empty_type_impl&) {
        return new empty_type_representation();
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
    void operator()(const counter_type_impl& t) {
        return visit(*long_type, native_value_delete_visitor{object});
    }
    void operator()(const empty_type_impl&) {
        delete reinterpret_cast<empty_type_representation*>(object);
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
    const std::type_info& operator()(const counter_type_impl& t) {
        return visit(*long_type, native_typeid_visitor{});
    }
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

bool operator==(const data_value& x, const data_value& y) {
    if (x._type != y._type) {
        return false;
    }
    if (x.is_null() && y.is_null()) {
        return true;
    }
    if (x.is_null() || y.is_null()) {
        return false;
    }
    return x._type->equal(*x.serialize(), *y.serialize());
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

bytes_opt data_value::serialize() const {
    if (!_value) {
        return std::nullopt;
    }
    bytes b(bytes::initialized_later(), serialized_size());
    auto i = b.begin();
    serialize(i);
    return b;
}

bytes data_value::serialize_nonnull() const {
    if (!_value) {
        on_internal_error(tlogger, "serialize_nonnull called on null");
    }
    return std::move(*serialize());
}

sstring abstract_type::get_string(const bytes& b) const {
    struct visitor {
        const bytes& b;
        sstring operator()(const abstract_type& t) {
            t.validate(b);
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

sstring user_type_impl::get_name_as_cql_string() const {
    return cql3::util::maybe_quote(get_name_as_string());
}

cql3::description user_type_impl::describe(cql3::with_create_statement with_create_statement) const {
    auto maybe_create_statement = std::invoke([&] -> std::optional<sstring> {
        if (!with_create_statement) {
            return std::nullopt;
        }

        std::ostringstream os;

        os << "CREATE TYPE " << cql3::util::maybe_quote(_keyspace) << "." << get_name_as_cql_string() << " (\n";
        for (size_t i = 0; i < _string_field_names.size(); i++) {
            os << "    " << cql3::util::maybe_quote(_string_field_names[i]) << " " << _types[i]->cql3_type_name();
            if (i < _string_field_names.size() - 1) {
                os << ",";
            }
            os << "\n";
        }
        os << ");";

        return std::move(os).str();
    });

    return cql3::description {
        .keyspace = _keyspace,
        .type = "type",
        .name = get_name_as_string(),
        .create_statement = std::move(maybe_create_statement)
    };
}

data_type
user_type_impl::freeze() const {
    if (_is_multi_cell) {
        return get_instance(_keyspace, _name, _field_names, _types, false);
    } else {
        return shared_from_this();
    }
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
    return std::move(os).str();
}

static std::optional<data_type> update_user_type_aux(
        const user_type_impl& u, const shared_ptr<const user_type_impl> updated) {
    if (u._keyspace == updated->_keyspace && u._name == updated->_name) {
        return std::make_optional<data_type>(u.is_multi_cell() ? updated : updated->freeze());
    }
    if (auto new_types = update_types(u.all_types(), updated)) {
        return std::make_optional(
                user_type_impl::get_instance(u._keyspace, u._name, u.field_names(), std::move(*new_types), u.is_multi_cell()));
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
        std::optional<data_type> operator()(const vector_type_impl& v) { return update_user_type_aux(v, updated); }
    };
    return visit(*this, visitor{updated});
}

static bytes_ostream serialize_for_cql_aux(const map_type_impl&, collection_mutation_view_description mut) {
    bytes_ostream out;
    auto len_slot = out.write_place_holder(collection_size_len());
    int elements = 0;
    for (auto&& e : mut.cells) {
        if (e.second.is_live(mut.tomb, false)) {
            write_collection_value(out, atomic_cell_value_view(e.first));
            write_collection_value(out, e.second.value());
            elements += 1;
        }
    }
    write_collection_size(len_slot, elements);
    return out;
}

static bytes_ostream serialize_for_cql_aux(const set_type_impl&, collection_mutation_view_description mut) {
    bytes_ostream out;
    auto len_slot = out.write_place_holder(collection_size_len());
    int elements = 0;
    for (auto&& e : mut.cells) {
        if (e.second.is_live(mut.tomb, false)) {
            write_collection_value(out, atomic_cell_value_view(e.first));
            elements += 1;
        }
    }
    write_collection_size(len_slot, elements);
    return out;
}

static bytes_ostream serialize_for_cql_aux(const list_type_impl&, collection_mutation_view_description mut) {
    bytes_ostream out;
    auto len_slot = out.write_place_holder(collection_size_len());
    int elements = 0;
    for (auto&& e : mut.cells) {
        if (e.second.is_live(mut.tomb, false)) {
            write_collection_value(out, e.second.value());
            elements += 1;
        }
    }
    write_collection_size(len_slot, elements);
    return out;
}

static bytes_ostream serialize_for_cql_aux(const user_type_impl& type, collection_mutation_view_description mut) {
    SCYLLA_ASSERT(type.is_multi_cell());
    SCYLLA_ASSERT(mut.cells.size() <= type.size());

    bytes_ostream out;

    size_t curr_field_pos = 0;
    for (auto&& e : mut.cells) {
        auto field_pos = deserialize_field_index(e.first);
        SCYLLA_ASSERT(field_pos < type.size());

        // Some fields don't have corresponding cells -- these fields are null.
        while (curr_field_pos < field_pos) {
            write_simple<int32_t>(out, int32_t(-1));
            ++curr_field_pos;
        }

        if (e.second.is_live(mut.tomb, false)) {
            auto value = e.second.value();
            write_simple<int32_t>(out, int32_t(value.size_bytes()));
            for (auto&& frag : fragment_range(value)) {
                out.write(frag);
            }
        } else {
            write_simple<int32_t>(out, int32_t(-1));
        }
        ++curr_field_pos;
    }

    // Trailing null fields
    while (curr_field_pos < type.size()) {
        write_simple<int32_t>(out, int32_t(-1));
        ++curr_field_pos;
    }

    return out;
}

bytes_ostream serialize_for_cql(const abstract_type& type, collection_mutation_view v) {
    SCYLLA_ASSERT(type.is_multi_cell());

    return v.with_deserialized(type, [&] (collection_mutation_view_description mv) {
        return visit(type, make_visitor(
            [&] (const map_type_impl& ctype) { return serialize_for_cql_aux(ctype, std::move(mv)); },
            [&] (const set_type_impl& ctype) { return serialize_for_cql_aux(ctype, std::move(mv)); },
            [&] (const list_type_impl& ctype) { return serialize_for_cql_aux(ctype, std::move(mv)); },
            [&] (const user_type_impl& utype) { return serialize_for_cql_aux(utype, std::move(mv)); },
            [&] (const abstract_type& o) -> bytes_ostream {
                throw std::runtime_error(format("attempted to serialize a collection of cells with type: {}", o.name()));
            }
        ));
    });
}

bytes serialize_field_index(size_t idx) {
    if (idx >= size_t(std::numeric_limits<int16_t>::max())) {
        // should've been rejected earlier, but just to be sure...
        throw std::runtime_error(format("index for user type field too large: {}", idx));
    }

    bytes b(bytes::initialized_later(), sizeof(int16_t));
    write_be(reinterpret_cast<char*>(b.data()), static_cast<int16_t>(idx));
    return b;
}

size_t deserialize_field_index(const bytes_view& b) {
    SCYLLA_ASSERT(b.size() == sizeof(int16_t));
    return read_be<int16_t>(reinterpret_cast<const char*>(b.data()));
}

size_t deserialize_field_index(managed_bytes_view b) {
    SCYLLA_ASSERT(b.size_bytes() == sizeof(int16_t));
    return be_to_cpu(read_simple_native<int16_t>(b));
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

data_value::data_value(sstring&& v) : data_value(make_new(utf8_type, std::move(v))) {
}

data_value::data_value(const char* v) : data_value(std::string_view(v)) {
}

data_value::data_value(std::string_view v) : data_value(sstring(v)) {
}

data_value::data_value(const std::string& v) : data_value(std::string_view(v)) {
}

data_value::data_value(const sstring& v) : data_value(std::string_view(v)) {
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

data_value::data_value(db_clock::time_point v) : data_value(make_new(timestamp_type, v)) {
}

data_value::data_value(time_native_type v) : data_value(make_new(time_type, v.nanoseconds)) {
}

data_value::data_value(timeuuid_native_type v) : data_value(make_new(timeuuid_type, v.uuid)) {
}

data_value::data_value(date_type_native_type v) : data_value(make_new(date_type, v.tp)) {
}

data_value::data_value(utils::multiprecision_int v) : data_value(make_new(varint_type, v)) {
}

data_value::data_value(big_decimal v) : data_value(make_new(decimal_type, v)) {
}

data_value::data_value(cql_duration d) : data_value(make_new(duration_type, d)) {
}

data_value::data_value(empty_type_representation e) : data_value(make_new(empty_type, e)) {
}

sstring data_value::to_parsable_string() const {
    // For some reason trying to do it using fmt::format refuses to compile
    // auto to_parsable_str_transform = std::views::transform([](const data_value& dv) -> sstring {
    //     return dv.to_parsable_string();
    // });

    if (_type->without_reversed().is_list()) {
        const list_type_impl::native_type* the_list = (const list_type_impl::native_type*)_value;
        std::ostringstream result;
        result << "[";
        for (size_t i = 0; i < the_list->size(); i++) {
            if (i != 0) {
                result << ", ";
            }

            result << (*the_list)[i].to_parsable_string();
        }
        result << "]";
        return std::move(result).str();
        //return fmt::format("[{}]", fmt::join(*the_list | to_parsable_str_transform, ", "));
    }

    if (_type->without_reversed().is_set()) {
        const set_type_impl::native_type* the_set = (const set_type_impl::native_type*)_value;
        std::ostringstream result;
        result << "{";
        for (size_t i = 0; i < the_set->size(); i++) {
            if (i != 0) {
                result << ", ";
            }

            result << (*the_set)[i].to_parsable_string();
        }
        result << "}";
        return std::move(result).str();
        //return fmt::format("{{{}}}", fmt::join(*the_set | to_parsable_str_transform, ", "));
    }

    if (_type->without_reversed().is_map()) {
        const map_type_impl::native_type* the_map = (const map_type_impl::native_type*)_value;
        std::ostringstream result;
        result << "{";
        for (size_t i = 0; i < the_map->size(); i++) {
            if (i != 0) {
                result << ", ";
            }

            result << (*the_map)[i].first.to_parsable_string() << ":" << (*the_map)[i].second.to_parsable_string();
        }
        result << "}";
        return std::move(result).str();
        //auto to_map_elem_transform = std::views::transform(
        //    [](const std::pair<data_value, data_value>& map_elem) -> sstring {
        //        return fmt::format("{{{}:{}}}", map_elem.first.to_parsable_string(), map_elem.second.to_parsable_string());
        //    }
        //);
        //
        //return fmt::format("{{{}}}", fmt::join(*the_map | to_map_elem_transform, ", "));
    }

    if (_type->without_reversed().is_user_type()) {
        const user_type_impl* user_typ = dynamic_cast<const user_type_impl*>(&_type->without_reversed());
        const user_type_impl::native_type* field_values = (const user_type_impl::native_type*)_value;
        std::ostringstream result;
        result << "{";

        for (std::size_t i = 0; i < field_values->size(); i++) {
            if (i != 0) {
                result << ", ";
            }
            result << user_typ->string_field_names().at(i) << ":" << (*field_values)[i].to_parsable_string();
        }
        result << "}";
        return std::move(result).str();
    }

    if (_type->without_reversed().is_tuple()) {
        const tuple_type_impl::native_type* tuple_elements = (const tuple_type_impl::native_type*)_value;
        std::ostringstream result;
        result << "(";

        for (std::size_t i = 0; i < tuple_elements->size(); i++) {
            if (i != 0) {
                result << ", ";
            }
            result << (*tuple_elements)[i].to_parsable_string();
        }
        result << ")";
        return std::move(result).str();
    }

    if (_type->without_reversed().is_vector()) {
        const vector_type_impl::native_type* vector_elements = (const vector_type_impl::native_type*)_value;
        std::ostringstream result;
        result << "[";
        for (std::size_t i = 0; i < vector_elements->size(); i++) {
            if (i != 0) {
                result << ", ";
            }
            result << (*vector_elements)[i].to_parsable_string();
        }
        result << "]";
        return std::move(result).str();
    }

    abstract_type::kind type_kind = _type->without_reversed().get_kind();

    if (type_kind == abstract_type::kind::utf8
        || type_kind == abstract_type::kind::ascii
        || type_kind == abstract_type::kind::inet
        || type_kind == abstract_type::kind::time
        || type_kind == abstract_type::kind::date
        || type_kind == abstract_type::kind::timestamp
    ) {
        // Put quotes on types that require it
        return fmt::format("'{}'", *this);
    }

    // For simple types the default operator<< should work ok
    return fmt::format("{}", *this);
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
make_vector_value(data_type type, vector_type_impl::native_type value) {
    return data_value::make_new(std::move(type), std::move(value));
}

data_value
make_user_value(data_type type, user_type_impl::native_type value) {
    return data_value::make_new(std::move(type), std::move(value));
}

auto fmt::formatter<data_value>::format(const data_value& v,
                                        fmt::format_context& ctx) const -> decltype(ctx.out()) {
    if (v.is_null()) {
        return fmt::format_to(ctx.out(), "null");
    }
    return fmt::format_to(ctx.out(), "{}", v.type()->to_string_impl(v));
}

shared_ptr<const reversed_type_impl> reversed_type_impl::get_instance(data_type type) {
    return intern::get_instance(std::move(type));
}

data_type reversed(data_type type) {
    if (type->is_reversed()) {
        return type->underlying_type();
    } else {
        return reversed_type_impl::get_instance(type);
    }
}

bool abstract_type::contains_set_or_map() const {
    return _contains_set_or_map;
}

bool abstract_type::contains_collection() const {
    return _contains_collection;
}

bool abstract_type::bound_value_needs_to_be_reserialized() const {
    // If a value contains set or map, then this collection can be sent in the wrong order
    // or there could be duplicates inside. We need to reserialize it into proper set or map.
    if (contains_set_or_map()) {
        return true;
    }

    return false;
}

// compile once the template instance that was externed in marshal_exception.hh
namespace seastar {
template void throw_with_backtrace<marshal_exception, sstring>(sstring&&);
}
