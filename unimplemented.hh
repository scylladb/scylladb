/*
 * Copyright 2015 Cloudius Systems
 */

#pragma once

#include <iostream>

namespace unimplemented {

static inline
void warn(sstring what) {
    std::cerr << "WARNING: Not implemented: " << what << std::endl;
}

static inline
void indexes() {
    warn("indexes");
}

}
