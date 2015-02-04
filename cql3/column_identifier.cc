/*
 * Copyright 2015 Cloudius Systems
 */

#include "cql3/column_identifier.hh"

namespace cql3 {

std::ostream& operator<<(std::ostream& out, const column_identifier::raw& id) {
    return out << id._text;
}

}
