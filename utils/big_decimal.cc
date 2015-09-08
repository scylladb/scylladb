/*
 * Copyright 2015 Cloudius Systems
 */

#include "big_decimal.hh"

#include <regex>

#include "types.hh"

big_decimal::big_decimal(sstring_view text)
{
    std::string str = text.to_string();
    static const std::regex big_decimal_re("^([\\+\\-]?)([0-9]*)(\\.([0-9]*))?([eE]([\\+\\-]?[0-9]+))?");
    std::smatch sm;
    if (!std::regex_match(str, sm, big_decimal_re)) {
        throw marshal_exception();
    }
    bool negative = sm[1] == "-";
    auto integer = sm[2].str();
    auto fraction = sm[4].str();
    auto exponent = sm[6].str();
    if (integer.empty() && fraction.empty()) {
        throw marshal_exception();
    }
    integer.append(fraction);
    unsigned i;
    for (i = 0; i < integer.size() - 1 && integer[i] == '0'; i++);
    integer = integer.substr(i);

    _unscaled_value = boost::multiprecision::cpp_int(integer);
    if (negative) {
        _unscaled_value *= -1;
    }
    _scale = exponent.empty() ? 0 : -boost::lexical_cast<int32_t>(exponent);
    _scale += fraction.size();
}

sstring big_decimal::to_string() const
{
    if (!_unscaled_value) {
        return "0";
    }
    boost::multiprecision::cpp_int num = boost::multiprecision::abs(_unscaled_value);
    auto str = num.str();
    if (_scale < 0) {
        for (int i = 0; i > _scale; i--) {
            str.push_back('0');
        }
    } else if (_scale > 0) {
        if (str.size() > unsigned(_scale)) {
            str.insert(str.size() - _scale, 1, '.');
        } else {
            std::string nstr = "0.";
            nstr.append(_scale - str.size(), '0');
            nstr.append(str);
            str = std::move(nstr);
        }

        while (str.back() == '0') {
            str.pop_back();
        }
        if (str.back() == '.') {
            str.pop_back();
        }
    }
    if (_unscaled_value < 0) {
        str.insert(0, 1, '-');
    }
    return str;
}

int big_decimal::compare(const big_decimal& other) const
{
    auto max_scale = std::max(_scale, other._scale);
    boost::multiprecision::cpp_int rescale(10);
    boost::multiprecision::cpp_int x = _unscaled_value * boost::multiprecision::pow(rescale, max_scale - _scale);
    boost::multiprecision::cpp_int y = other._unscaled_value * boost::multiprecision::pow(rescale, max_scale - other._scale);
    return x == y ? 0 : x < y ? -1 : 1;
}