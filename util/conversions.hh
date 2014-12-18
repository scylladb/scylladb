/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 */

#ifndef CONVERSIONS_HH_
#define CONVERSIONS_HH_

#include <cstdlib>
#include <string>
#include <vector>

// Convert a string to a memory size, allowing binary SI
// suffixes (intentionally, even though SI suffixes are
// decimal, to follow existing usage).
//
// "5" -> 5
// "4k" -> (4 << 10)
// "8M" -> (8 << 20)
// "7G" -> (7 << 30)
// "1T" -> (1 << 40)
// anything else: exception
size_t parse_memory_size(std::string s);

static inline std::vector<char> string2vector(std::string str) {
    auto v = std::vector<char>(str.begin(), str.end());
    v.push_back('\0');
    return v;
}

#endif /* CONVERSIONS_HH_ */
