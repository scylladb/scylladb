/*
 * Copyright 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "db/tags/utils.hh"

#include "db/tags/extension.hh"
#include "schema/schema_builder.hh"
#include "schema/schema_registry.hh"
#include "service/storage_proxy.hh"
#include "data_dictionary/data_dictionary.hh"

static logging::logger tlogger("tags");

namespace db {

const std::map<sstring, sstring>* get_tags_of_table(schema_ptr schema) {
    auto tags_ext = get_schema_extension<tags_extension>(schema->extensions(), tags_extension::NAME);
    return tags_ext ? &tags_ext->tags() : nullptr;
}

std::optional<std::string> find_tag(const schema& s, const sstring& tag) {
    auto ext = get_schema_extension<tags_extension>(s.extensions(), tags_extension::NAME);
    if (!ext) {
        return std::nullopt;
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
        size_t retries = mm.get_concurrent_ddl_retries();
        for (;;) {
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
            try {
                co_await mm.announce(std::move(m), std::move(group0_guard), format("Modify tags for {} table", cf));
                break;
            }  catch (const service::group0_concurrent_modification& ex) {
                tlogger.info("Failed to modify tags for table {} due to concurrent schema modifications. {}.",
                    cf, retries ? "Retrying" : "Number of retries exceeded, giving up");
                if (retries--) {
                    continue;
                }
                throw;
            }
        }
    });
}

}
