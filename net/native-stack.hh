/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 */

#ifndef STACK_HH_
#define STACK_HH_

#include "core/reactor.hh"
#include <boost/program_options.hpp>

namespace net {

boost::program_options::options_description
native_network_stack_program_options();

std::unique_ptr<network_stack>
create_native_network_stack(boost::program_options::options_description opts);

}

#endif /* STACK_HH_ */
