/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 */

#ifndef STACK_HH_
#define STACK_HH_

#include "core/reactor.hh"

namespace net {

std::unique_ptr<networking_stack>
create_native_networking_stack();

}

#endif /* STACK_HH_ */
