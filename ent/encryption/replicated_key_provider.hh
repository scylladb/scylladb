/*
 * Copyright (C) 2015 ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include "encryption.hh"

namespace db {
class extensions;
}

namespace replica {
class database;
}

namespace service {
class migration_manager;
}

namespace encryption {

class replicated_key_provider_factory : public key_provider_factory {
public:
    replicated_key_provider_factory();
    ~replicated_key_provider_factory();

    shared_ptr<key_provider> get_provider(encryption_context&, const options&) override;

    static void init(db::extensions&);
    static future<> on_started(::replica::database&, service::migration_manager&);
};

}
