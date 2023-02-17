/*
 * SPDX-FileCopyrightText: 2023 ScyllaDB
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */
#include <bit>
#include <bitset>
#include <limits>

#define BOOST_TEST_MODULE type_json

#include <boost/test/unit_test.hpp>

#include "concrete_types.hh"
#include "cql3/type_json.hh"

class IEEE754_double {
    static_assert(std::numeric_limits<double>::is_iec559);
    static constexpr std::size_t BITS_SIGN = 1;
    static constexpr std::size_t BITS_EXPONENT =
        std::bit_width(static_cast<unsigned>(std::numeric_limits<double>::max_exponent -
                                             std::numeric_limits<double>::min_exponent));
    // the leading bit is implicit, so remove it
    static constexpr std::size_t BITS_MANTISSA =
        std::numeric_limits<double>::digits - 1;
    static constexpr std::size_t BITS_ALL = BITS_SIGN + BITS_EXPONENT + BITS_MANTISSA;
    static_assert(BITS_ALL == sizeof(double) * CHAR_BIT);

    std::bitset<BITS_ALL> _bits;

    explicit IEEE754_double(uint64_t n)
        : _bits(n) {}

    bool is_negative() const {
        return _bits.test(BITS_SIGN + BITS_EXPONENT + BITS_MANTISSA - 1);
    }

    std::bitset<BITS_ALL> cut(std::size_t pos, std::size_t count, bool fixed) const {
        auto bits = _bits;
        bits <<= pos;
        bits >>= pos;
        std::size_t bits_tail = BITS_ALL - pos - count;
        bits >>= bits_tail;
        if (fixed) {
            bits <<= bits_tail;
        }
        return bits;
    }

    void rewrite(std::size_t pos, std::size_t count, const std::bitset<BITS_ALL>& bits) {
        auto head = cut(0, pos, true);
        auto trimmed_bits = bits;
        trimmed_bits <<= BITS_ALL - count;
        trimmed_bits >>= pos;
        std::size_t tail_pos = pos + count;
        auto tail = cut(tail_pos, BITS_ALL - tail_pos, true);
        _bits = head | trimmed_bits | tail;
    }

    uint32_t exponent() const {
        return cut(BITS_SIGN, BITS_EXPONENT, false).to_ulong();
    }

    void set_exponent(uint32_t e) {
        rewrite(BITS_SIGN, BITS_EXPONENT, std::bitset<BITS_ALL>(e));
    }

    std::bitset<BITS_ALL> mantissa_bits() const {
        return cut(BITS_SIGN + BITS_EXPONENT, BITS_MANTISSA, false);
    }

    uint64_t mantissa() const {
        return mantissa_bits().to_ulong();
    }

    void set_mantissa(uint64_t m) {
        rewrite(BITS_SIGN + BITS_EXPONENT, BITS_MANTISSA, std::bitset<BITS_ALL>(m));
    }

    double next_abs_greater() const {
        auto result = *this;
        if (mantissa_bits().all()) {
            // "carry" a power of 2
            result.set_exponent(exponent() + 1);
            result.set_mantissa(0);
        } else {
            result.set_mantissa(mantissa() + 1);
        }
        return double(result);
    }

    double next_abs_lesser() const {
        auto result = *this;
        if (mantissa_bits().none()) {
            // "borrow" a power of 2
            result.set_exponent(exponent() - 1);
            result.set_mantissa(std::numeric_limits<uint64_t>::max());
        } else {
            result.set_mantissa(mantissa() - 1);
        }
        return double(result);
    }

public:
    // construct a 64-bit float point number which is closest to
    // the specified "n" by casting it to double
    static IEEE754_double closest(int64_t n) {
        static_assert(std::numeric_limits<double>::round_style == std::float_round_style::round_to_nearest);
        auto fp64 = static_cast<double>(n);
        return IEEE754_double(std::bit_cast<uint64_t>(fp64));
    }

    explicit operator double() const {
        return std::bit_cast<double>(_bits.to_ulong());
    }

    // returns the maximum number less than "n"
    double previous() const {
        if (is_negative()) {
            return next_abs_greater();
        } else {
            return next_abs_lesser();
        }
    }

    double next() const {
        if (is_negative()) {
            return next_abs_lesser();
        } else {
            return next_abs_greater();
        }
    }
};

BOOST_AUTO_TEST_CASE(test_from_json_object) {
  // double, which is IEEE-754 64 float point. despite that it can cover a wide
  // dynamic range of numeric values, but its precision not high enough to represent
  // int64_t, so we need to be pay more attention when checking if a double is
  // std::in_range<int64_t>().
  static_assert(sizeof(double) == sizeof(int64_t));
  auto int64_type = make_shared<long_type_impl>();

  auto closest_to_max = IEEE754_double::closest(std::numeric_limits<int64_t>::max());
  // from_json_object() should not throw at seeing: std::numeric_limits<int64_t>::max() - 1
  //
  // well sort of, as the 64 bit float point is not to present this number, we pick the
  // last greatest 64 bit float point number less than std::numeric_limits<int64_t>::max() .
  rjson::value less_than_int64_t_max{closest_to_max.previous()};
  BOOST_CHECK_NO_THROW(from_json_object(*int64_type, less_than_int64_t_max));
  // we don't test std::numeric_limits<int64_t>::max() here. because 64 bit IEEE-754 float
  // point is not able to represent std::numeric_limits<int64_t>::max(),
  // i.e., 9223372036854775807. if we cast it to double, we'll have a closest float number,
  // i.e., 9223372036854775808.

  // from_json_object() should throw at seeing: std::numeric_limits<int64_t>::max() + 1
  //
  // because the closest number 9223372036854775808 cannot be represented by a 64 bit fixed
  // integer, i.e., int64_t. we can just use it for testing the out-of-bound case.
  rjson::value greater_than_int64_t_max{static_cast<double>(closest_to_max)};
  BOOST_CHECK_THROW(from_json_object(*int64_type, greater_than_int64_t_max),
                    marshal_exception);

  auto closest_to_min = IEEE754_double::closest(std::numeric_limits<int64_t>::min());
  // from_json_object() should throw at seeing: std::numeric_limits<int64_t>::min() - 1
  //
  // we pick the greatest 64 bit float point number less than
  // std::numeric_limits<int64_t>::min()
  rjson::value less_than_int64_t_min(closest_to_min.previous());
  BOOST_CHECK_THROW(from_json_object(*int64_type, less_than_int64_t_min),
                    marshal_exception);

  // fortunately double is capable of representing 0x8000000000000000
  rjson::value equal_to_int64_t_min(static_cast<double>(closest_to_min));
  BOOST_CHECK_NO_THROW(from_json_object(*int64_type, equal_to_int64_t_min));

  // from_json_object() should not throw at seeing: std::numeric_limits<int64_t>::min()
  //
  // we pick the least 64 bit float point number greater than
  // std::numeric_limits<int64_t>::min()
  rjson::value greater_than_int64_t_min(closest_to_min.next());
  BOOST_CHECK_NO_THROW(from_json_object(*int64_type, greater_than_int64_t_min));
}
