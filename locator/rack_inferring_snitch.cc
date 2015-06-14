#include "locator/rack_inferring_snitch.hh"

namespace locator {
using registry = class_registrator<i_endpoint_snitch, rack_inferring_snitch>;
static registry registrator("org.apache.cassandra.locator.RackInferringSnitch");
}
