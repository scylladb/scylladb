/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "replica/database.hh"
#include "service/migration_manager.hh"
#include <any>

namespace service {

class tablet_allocator_impl;

class tablet_allocator {
public:
    class impl {
    public:
        virtual ~impl() = default;
    };
private:
    std::unique_ptr<impl> _impl;
    tablet_allocator_impl& impl();
public:
    tablet_allocator(service::migration_notifier& mn, replica::database& db);
public:
    future<> stop();
};

}
