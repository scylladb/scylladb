
/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "locator/simple_snitch.hh"
#include "utils/class_registrator.hh"

namespace locator {
using registry = class_registrator<i_endpoint_snitch, simple_snitch, const snitch_config&>;
static registry registrator1("org.apache.cassandra.locator.SimpleSnitch");
static registry registrator2("SimpleSnitch");
}
