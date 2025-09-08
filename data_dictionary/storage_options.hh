/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <filesystem>
#include <map>
#include <variant>
#include <seastar/core/sstring.hh>
#include "schema/schema_fwd.hh"
#include "utils/s3/utils/manip_s3.hh"
#include "seastarx.hh"

namespace seastar {
    class abort_source;
}

namespace data_dictionary {

struct storage_options {
    struct local {
        std::filesystem::path dir;
        std::map<sstring, sstring> to_map() const;
        std::string_view name() const;
        bool operator==(const local&) const = default;
    };
    struct object_storage {
        std::string bucket;
        std::string endpoint;
        std::variant<sstring, table_id> location;
        seastar::abort_source* abort_source = nullptr;

        std::string type;

        std::map<sstring, sstring> to_map() const;
        std::string_view name() const;
        bool operator==(const object_storage&) const;
    };
    using s3 = object_storage;
    using gs = object_storage;

    using value_type = std::variant<local, object_storage>;
    value_type value = local{};

    storage_options() = default;

    bool is_local_type() const noexcept;
    bool is_object_storage_type() const noexcept;

    bool is_s3_type() const noexcept;
    bool is_gs_type() const noexcept;

    std::string_view type_string() const;
    std::map<sstring, sstring> to_map() const;

    bool can_update_to(const storage_options& new_options);

    static value_type from_map(std::string_view type, const std::map<sstring, sstring>& values);

    static const std::string LOCAL_NAME;
    static const std::string S3_NAME;
    static const std::string GS_NAME;

    storage_options append_to_object_storage_prefix(const sstring& s) const;
};

storage_options make_local_options(std::filesystem::path dir);
storage_options make_object_storage_options(const std::string& endpoint, const std::string& fqn, abort_source* = nullptr);
storage_options make_object_storage_options(const std::string& endpoint, const std::string& type, const std::string& bucket, const std::string& prefix, abort_source* = nullptr);

bool is_object_storage_fqn(const std::filesystem::path& fqn, std::string_view type);
bool object_storage_fqn_to_parts(const std::filesystem::path& fqn, std::string_view type, std::string& bucket_name, std::string& object_name);

} // namespace data_dictionary

template <>
struct fmt::formatter<data_dictionary::storage_options> {
    constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }
    auto format(const data_dictionary::storage_options&, fmt::format_context& ctx) const -> decltype(ctx.out());
};
