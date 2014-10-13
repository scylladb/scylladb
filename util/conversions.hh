/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 */

#ifndef CONVERSIONS_HH_
#define CONVERSIONS_HH_

#include <cstdlib>
#include <string>

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


#endif /* CONVERSIONS_HH_ */
