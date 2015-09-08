/*
 * Copyright 2015 Cloudius Systems
 */

#pragma once

#include <boost/multiprecision/cpp_int.hpp>

#include "bytes.hh"

class big_decimal {
private:
    int32_t _scale;
    boost::multiprecision::cpp_int _unscaled_value;
public:
    big_decimal(sstring_view text);
    big_decimal(int32_t scale, boost::multiprecision::cpp_int unscaled_value)
        : _scale(scale), _unscaled_value(unscaled_value)
    { }

    int32_t scale() const { return _scale; }
    const boost::multiprecision::cpp_int& unscaled_value() const { return _unscaled_value; }

    sstring to_string() const;

    int compare(const big_decimal& other) const;
};
