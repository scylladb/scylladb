/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include "utils/UUID.hh"

using node_ops_id = utils::tagged_uuid<struct node_ops_id_tag>;
