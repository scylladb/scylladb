
/*
 * Copyright 2015 Cloudius Systems
 */

#include "locator/simple_snitch.hh"
#include "utils/class_registrator.hh"

namespace locator {
using registry = class_registrator<i_endpoint_snitch, simple_snitch>;
static registry registrator1("org.apache.cassandra.locator.SimpleSnitch");
static registry registrator2("SimpleSnitch");
}
