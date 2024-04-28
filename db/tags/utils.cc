/*
 * Copyright 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "db/tags/utils.hh"

#include "db/tags/extension.hh"
#include "schema/schema_builder.hh"
#include "schema/schema_registry.hh"
#include <seastar/core/on_internal_error.hh>
#include "service/storage_proxy.hh"
#include "data_dictionary/data_dictionary.hh"

static logging::logger tlogger("tags");

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
    auto ext = dynamic_pointer_cast<tags_extension>(it1->second);
    if (!ext) {
        on_internal_error(tlogger, fmt::format("tag extension found in table {}.{}, but has wrong type", s.ks_name(), s.cf_name()));
    }
    const std::map<sstring, sstring>& tags_map = ext->tags();
    auto it2 = tags_map.find(tag);
    if (it2 == tags_map.end()) {
        return std::nullopt;
    } else {
        return it2->second;
    }
}

future<> modify_tags(service::migration_manager& mm, sstring ks, sstring cf,
                     std::function<void(std::map<sstring, sstring>&)> modify) {
    co_await mm.container().invoke_on(0, [ks = std::move(ks), cf = std::move(cf), modify = std::move(modify)] (service::migration_manager& mm) -> future<> {
        // FIXME: the following needs to be in a loop. If mm.announce() below
        // fails, we need to retry the whole thing.
        auto group0_guard = co_await mm.start_group0_operation();
        // After getting the schema-modification lock, we need to read the
        // table's *current* schema - it might have changed before we got
        // the lock, by some concurrent modification. If the table is gone,
        // this will throw no_such_column_family.
        schema_ptr s = mm.get_storage_proxy().data_dictionary().find_schema(ks, cf);
        const std::map<sstring, sstring>* tags_ptr = get_tags_of_table(s);
        std::map<sstring, sstring> tags;
        if (tags_ptr) {
            // tags_ptr is a constant pointer to schema data. To allow func()
            // to modify the tags, we must make a copy.
            tags = *tags_ptr;
        }
        modify(tags);
        schema_builder builder(s);
        builder.add_extension(tags_extension::NAME, ::make_shared<tags_extension>(tags));

        auto m = co_await service::prepare_column_family_update_announcement(mm.get_storage_proxy(),
                builder.build(), std::vector<view_ptr>(), group0_guard.write_timestamp());

        co_await mm.announce(std::move(m), std::move(group0_guard), format("Modify tags for {} table", cf));
    });
}

}
