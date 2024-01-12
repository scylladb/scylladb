/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <seastar/util/bool_class.hh>

using namespace seastar;

using maintenance_socket_enabled = bool_class<class maintenance_socket_enabled_tag>;
using maintenance_mode_enabled = bool_class<class maintenance_mode_enabled_tag>;
