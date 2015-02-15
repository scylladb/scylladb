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
template<typename PtrToPrintable>
static inline
sstring
to_string(std::vector<PtrToPrintable> items) {
    // TODO: optimize
    std::ostringstream oss;
    int left = items.size();
    oss << "[";
    for (auto&& item : items) {
        oss << item->to_string();
        if (left != 1) {
            oss << ", ";
        }
        --left;
    }
    oss << "]";
    return oss.str();
}
