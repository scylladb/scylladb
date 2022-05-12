/*
 * Copyright (C) 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#include "cql3/selection/simple_selector.hh"

namespace cql3 {

namespace selection {

::shared_ptr<selector>
simple_selector_factory::new_instance() const {
    return ::make_shared<simple_selector>(_column_name, _idx, _type);
}

}

}
