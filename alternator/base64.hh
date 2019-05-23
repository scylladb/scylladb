/*
 * Copyright 2019 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include <string>
#include "bytes.hh"

std::string base64_encode(bytes_view);
bytes base64_decode(std::string_view);
