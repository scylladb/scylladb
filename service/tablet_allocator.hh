/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "replica/database.hh"
#include "service/migration_manager.hh"
#include "locator/tablets.hh"
#include <any>

namespace service {

/// Represents intention to move a single tablet replica from src to dst.
struct tablet_migration_info {
    locator::global_tablet_id tablet;
    locator::tablet_replica src;
    locator::tablet_replica dst;
};

using migration_plan = utils::chunked_vector<tablet_migration_info>;

/// Returns a tablet migration plan that aims to achieve better load balance in the whole cluster.
/// The plan is computed based on information in the given token_metadata snapshot
/// and thus should be executed and reflected, at least as pending tablet transitions, in token_metadata
/// before this is called again.
///
/// For any given global_tablet_id there is at most one tablet_migration_info in the returned plan.
///
/// To achieve full balance, do:
///
///    while (true) {
///        auto plan = co_await balance_tablets(get_token_metadata());
///        if (plan.empty()) {
///            break;
///        }
///        co_await execute(plan);
///    }
///
future<migration_plan> balance_tablets(locator::token_metadata_ptr);

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

template <>
struct fmt::formatter<service::tablet_migration_info> : fmt::formatter<std::string_view> {
    auto format(const service::tablet_migration_info&, fmt::format_context& ctx) const -> decltype(ctx.out());
};
