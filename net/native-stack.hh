/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 */

#ifndef STACK_HH_
#define STACK_HH_

#include "net/net.hh"
#include <boost/program_options.hpp>

namespace net {

void create_native_stack(boost::program_options::variables_map opts, std::shared_ptr<device> dev);

}

#endif /* STACK_HH_ */
