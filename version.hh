#pragma once

#include "core/sstring.hh"
#include "core/print.hh"
#include <tuple>

namespace version {
class version {
    std::tuple<uint16_t, uint16_t, uint16_t> _version;
public:
    version(uint16_t x, uint16_t y = 0, uint16_t z = 0): _version(std::make_tuple(x, y, z)) {}

    sstring to_sstring() {
        return sprint("%d.%d.%d", std::get<0>(_version), std::get<1>(_version), std::get<2>(_version));
    }

    static version current() {
        static version v(2, 1, 8);
        return v;
    }

    bool operator==(version v) const {
        return _version == v._version;
    }

    bool operator!=(version v) const {
        return _version != v._version;
    }

    bool operator<(version v) const {
        return _version < v._version;
    }
    bool operator<=(version v) {
        return _version <= v._version;
    }
    bool operator>(version v) {
        return _version > v._version;
    }
    bool operator>=(version v) {
        return _version >= v._version;
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
