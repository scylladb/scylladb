/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "selector.hh"
#include "cql3/column_identifier.hh"

namespace cql3 {

namespace selection {

lw_shared_ptr<column_specification>
selector::factory::get_column_specification(const schema& schema) const {
    return make_lw_shared<column_specification>(schema.ks_name(),
        schema.cf_name(),
        ::make_shared<column_identifier>(column_name(), true),
        get_return_type());
}

bool selector::requires_thread() const { return false; }
}

}


