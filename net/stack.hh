/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 */

#ifndef STACK_HH_
#define STACK_HH_

#include "core/reactor.hh"

namespace net {

std::unique_ptr<network_stack>
create_native_network_stack();

}

#endif /* STACK_HH_ */
