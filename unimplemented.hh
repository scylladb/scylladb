/*
 * Copyright 2015 Cloudius Systems
 */

#pragma once

#include <iostream>
#include "core/print.hh"
#include "core/sstring.hh"

namespace unimplemented {

static inline
void fail(sstring what) __attribute__((noreturn));

static inline
void fail(sstring what) {
    throw std::runtime_error(sprint("not implemented: %s", what));
}

void indexes();

static inline
void lwt() __attribute__((noreturn));

static inline
void lwt() {
    fail("light-weight transactions");
}

void auth();
void permissions();
void triggers();

static inline
void collections() __attribute__((noreturn));

static inline
void collections() {
    fail("collections");
}

static inline
void metrics() {}

static inline
void compact_tables() {}

}
