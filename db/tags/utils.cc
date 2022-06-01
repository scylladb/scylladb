/*
 * Copyright 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "db/tags/utils.hh"

#include "db/tags/extension.hh"
#include "schema_builder.hh"
#include "schema_registry.hh"

namespace db {

const std::map<sstring, sstring>* get_tags_of_table(schema_ptr schema) {
    auto it = schema->extensions().find(tags_extension::NAME);
    if (it == schema->extensions().end()) {
        return nullptr;
    }
    auto tags_ext = static_pointer_cast<tags_extension>(it->second);
    return &tags_ext->tags();
}

std::optional<std::string> find_tag(const schema& s, const sstring& tag) {
    auto it1 = s.extensions().find(tags_extension::NAME);
    if (it1 == s.extensions().end()) {
        return std::nullopt;
    }
    const std::map<sstring, sstring>& tags_map =
        static_pointer_cast<tags_extension>(it1->second)->tags();
    auto it2 = tags_map.find(tag);
    if (it2 == tags_map.end()) {
        return std::nullopt;
    } else {
        return it2->second;
    }
}

future<> update_tags(service::migration_manager& mm, schema_ptr schema, std::map<sstring, sstring>&& tags_map) {
    co_await mm.container().invoke_on(0, [s = global_schema_ptr(std::move(schema)), tags_map = std::move(tags_map)] (service::migration_manager& mm) -> future<> {
        // FIXME: the following needs to be in a loop. If mm.announce() below
        // fails, we need to retry the whole thing.
        auto group0_guard = co_await mm.start_group0_operation();

        schema_builder builder(s);
        builder.add_extension(tags_extension::NAME, ::make_shared<tags_extension>(tags_map));

        auto m = co_await mm.prepare_column_family_update_announcement(builder.build(), false, std::vector<view_ptr>(), group0_guard.write_timestamp());

        co_await mm.announce(std::move(m), std::move(group0_guard));
    });
}

}
