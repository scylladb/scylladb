#include "locator/simple_snitch.hh"
#include "utils/class_registrator.hh"

namespace locator {
using registry = class_registrator<i_endpoint_snitch, simple_snitch>;
static registry registrator("org.apache.cassandra.locator.SimpleSnitch");
}
