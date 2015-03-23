/*
 * Copyright 2015 Cloudius Systems
 */

#pragma once

#include "core/sstring.hh"
#include <vector>
#include <sstream>

/**
 * Converts a vector of pointers to Printable elements.
 * Printable is an object which has to_string() method.
 */
template<typename Printable>
static inline
sstring
to_string(const std::vector<Printable>& items) {
    // TODO: optimize
    std::ostringstream oss;
    size_t left = items.size();
    oss << "[";
    for (auto&& item : items) {
        oss << item;
        if (left != 1) {
            oss << ", ";
        }
        --left;
    }
    oss << "]";
    return oss.str();
}

template<typename Printable>
static inline
sstring join(sstring delimiter, const std::vector<Printable>& items) {
    std::ostringstream oss;
    size_t left = items.size();
    for (auto&& item : items) {
        oss << item;
        if (left != 1) {
            oss << delimiter;
        }
        --left;
    }
    return oss.str();
}
