/*
 * Copyright 2017-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

namespace db {

enum class consistency_level : uint8_t {
    ANY,
    ONE,
    TWO,
    THREE,
    QUORUM,
    ALL,
    LOCAL_QUORUM,
    EACH_QUORUM,
    SERIAL,
    LOCAL_SERIAL,
    LOCAL_ONE
};

}
