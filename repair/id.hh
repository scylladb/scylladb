/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "utils/UUID.hh"

using node_ops_id = utils::tagged_uuid<struct node_ops_id_tag>;

struct repair_uniq_id {
    // The integer ID used to identify a repair job. It is currently used by nodetool and http API.
    int id;
    // A UUID to identifiy a repair job. We will transit to use UUID over the integer ID.
    utils::UUID uuid;
};
std::ostream& operator<<(std::ostream& os, const repair_uniq_id& x);
