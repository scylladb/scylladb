/*
 * Copyright (C) 2015 Cloudius Systems, Ltd.
 */

#include "unimplemented.hh"
#include "core/sstring.hh"

namespace unimplemented {

static inline
void warn(sstring what) {
    std::cerr << "WARNING: Not implemented: " << what << std::endl;
}

class warn_once {
    sstring _msg;
public:
    warn_once(const char* msg) : _msg(msg) {}
    void operator()() {
        if (!_msg.empty()) {
            warn(_msg);
            _msg.reset();
        }
    }
};

void indexes() {
    static thread_local warn_once w("indexes");
    w();
}

void auth() {
    static thread_local warn_once w("auth");
    w();
}

void permissions() {
    static thread_local warn_once w("permissions");
    w();
}

void triggers() {
    static thread_local warn_once w("triggers");
    w();
}

}
