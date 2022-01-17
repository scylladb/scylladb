/*
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "utils/error_injection.hh"

namespace utils {

logging::logger errinj_logger("debug_error_injection");

thread_local error_injection<false> error_injection<false>::_local;

} // namespace utils
