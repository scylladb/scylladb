#pragma once

#include "core/sstring.hh"
#include "core/print.hh"

namespace version {
class version {
    uint8_t maj;
    uint8_t min;
    uint16_t rev;

public:
    version(uint8_t x, uint8_t y = 0, uint16_t z = 0): maj(x), min(y), rev(z) {}

    sstring to_sstring() {
        return sprint("%d.%d.%d", maj, min, rev);
    }

    static version current() {
        static version v(2, 1, 8);
        return v;
    }

    bool operator==(version v) const {
        return (maj == v.maj) && (min == v.min) && (rev == v.rev);
    }

    bool operator!=(version v) const {
        return !(v == *this);
    }

    bool operator<(version v) const {
        if (maj < v.maj) {
            return true;
        } else if (maj > v.maj) {
            return false;
        }

        if (min < v.min) {
            return true;
        } else if (min > v.min) {
            return false;
        }
        return rev < v.rev;
    }
    bool operator<=(version v) {
        return ((*this < v) || (*this == v));
    }
    bool operator>(version v) {
        return !(*this <= v);
    }
    bool operator>=(version v) {
        return ((*this == v) || !(*this < v));
    }
};

inline const int native_protocol() {
    return 3;
}

inline const sstring& release() {
    static thread_local auto str_ver = version::current().to_sstring();
    return str_ver;
}
}
