#include "cql3/constants.hh"

namespace cql3 {

const ::shared_ptr<term::raw> constants::NULL_LITERAL = ::make_shared<constants::null_literal>();
const ::shared_ptr<terminal> constants::null_literal::NULL_VALUE = ::make_shared<constants::null_literal::null_value>();

}