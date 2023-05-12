/*
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma  once

#include <seastar/core/shared_ptr.hh>
#include <seastar/core/distributed.hh>
#include "service/load_broadcaster.hh"

using namespace seastar;

namespace replica {
class database;
}

namespace gms { class gossiper; }

namespace service {

class load_meter {
private:
    shared_ptr<load_broadcaster> _lb;

    /** raw load value */
    double get_load() const;

public:
    future<std::map<sstring, double>> get_load_map();

    future<> init(distributed<replica::database>& db, gms::gossiper& gossiper);
    future<> exit();
};

}
