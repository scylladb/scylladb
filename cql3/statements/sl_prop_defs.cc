/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#include "cql3/statements/sl_prop_defs.hh"
#include "database.hh"


namespace cql3 {

namespace statements {

void sl_prop_defs::validate() {
    property_definitions::validate({});
}

}

}
