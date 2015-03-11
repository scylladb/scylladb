#include <boost/range/algorithm/transform.hpp>
#include <boost/range/algorithm.hpp>
#include <boost/range/adaptors.hpp>
#include "cql3/restrictions/statement_restrictions.hh"

namespace cql3 {
namespace restrictions {

using boost::adaptors::filtered;
using boost::adaptors::transformed;

std::vector<::shared_ptr<column_identifier>>
statement_restrictions::get_partition_key_unrestricted_components() const {
    std::vector<::shared_ptr<column_identifier>> r;

    auto restricted = _partition_key_restrictions->get_column_defs();
    auto is_not_restricted = [&restricted] (const column_definition& def) {
        return !boost::count(restricted, &def);
    };

    boost::copy(_schema->partition_key_columns() | filtered(is_not_restricted) | transformed(to_identifier),
        std::back_inserter(r));
    return r;
}

}
}
