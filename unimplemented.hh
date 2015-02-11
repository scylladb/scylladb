/*
 * Copyright 2015 Cloudius Systems
 */

#pragma once

#include <iostream>
#include "core/print.hh"

namespace unimplemented {

static inline
void fail(sstring what) __attribute__((noreturn));

static inline
void fail(sstring what) {
    throw std::runtime_error(sprint("not implemented: %s", what));
}

static inline
void warn(sstring what) {
    std::cerr << "WARNING: Not implemented: " << what << std::endl;
}

static inline
void indexes() {
    warn("indexes");
}

static inline
void lwt() __attribute__((noreturn));

static inline
void lwt() {
    fail("light-weight transactions");
}

static inline
void auth() {
    warn("auth");
}

static inline
void permissions() {
    warn("permissions");
}

static inline
void triggers() {
    warn("triggers");
}

}
