/*
 * Copyright 2019-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <string_view>
#include "bytes.hh"

std::string base64_encode(bytes_view);

bytes base64_decode(std::string_view);

size_t base64_decoded_len(std::string_view str);

bool base64_begins_with(std::string_view base, std::string_view operand);
