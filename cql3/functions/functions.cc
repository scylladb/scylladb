/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 */

#include "functions.hh"

namespace cql3 {
namespace functions {

thread_local std::unordered_multimap<function_name, shared_ptr<function>> functions::_declared = init();

}
}


