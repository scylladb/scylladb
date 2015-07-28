#pragma once

#include "core/sstring.hh"

namespace version {
inline const int native_protocol() {
    return 3;
}

inline const sstring& release() {
    static sstring v = "2.1.8";
    return v;
}
}
