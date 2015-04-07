/*
 * Copyright (C) 2015 Cloudius Systems, Ltd.
 */

#include "selector.hh"
#include "cql3/column_identifier.hh"

namespace cql3 {

namespace selection {

::shared_ptr<column_specification>
selector::factory::get_column_specification(schema_ptr schema) {
    return ::make_shared<column_specification>(schema->ks_name,
        schema->cf_name,
        ::make_shared<column_identifier>(column_name(), true),
        get_return_type());
}

}

}


