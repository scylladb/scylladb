/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 */

#ifndef CONVERSIONS_CC_
#define CONVERSIONS_CC_

#include "conversions.hh"
#include "core/print.hh"
#include <boost/lexical_cast.hpp>

size_t parse_memory_size(std::string s) {
    size_t factor = 1;
    if (s.size()) {
        auto c = s[s.size() - 1];
        static std::string suffixes = "kMGT";
        auto pos = suffixes.find(c);
        if (pos == suffixes.npos) {
            throw std::runtime_error(sprint("Cannot parse memory size '%s'", s));
        }
        factor <<= (pos + 1) * 10;
        s = s.substr(0, s.size() - 1);
    }
    return boost::lexical_cast<size_t>(s) * factor;
}


#endif /* CONVERSIONS_CC_ */
