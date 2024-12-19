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
#include "seastarx.hh"

namespace seastar {
    class abort_source;
}

namespace data_dictionary {

struct storage_options {
    struct local {
        std::filesystem::path dir;
        static constexpr std::string_view name = "LOCAL";

        static local from_map(const std::map<sstring, sstring>&);
        std::map<sstring, sstring> to_map() const;
        bool operator==(const local&) const = default;
    };
    struct s3 {
        sstring bucket;
        sstring endpoint;
        std::variant<sstring, table_id> location;
        seastar::abort_source* abort_source = nullptr;
        static constexpr std::string_view name = "S3";

        static s3 from_map(const std::map<sstring, sstring>&);
        std::map<sstring, sstring> to_map() const;
        bool operator==(const s3&) const = default;
    };
    using value_type = std::variant<local, s3>;
    value_type value = local{};

    storage_options() = default;

    bool is_local_type() const noexcept;
    std::string_view type_string() const;
    std::map<sstring, sstring> to_map() const;

    bool can_update_to(const storage_options& new_options);

    static value_type from_map(std::string_view type, std::map<sstring, sstring> values);

    storage_options append_to_s3_prefix(const sstring& s) const;
};

inline storage_options make_local_options(std::filesystem::path dir) {
    storage_options so;
    so.value = data_dictionary::storage_options::local { .dir = std::move(dir) };
    return so;
}

} // namespace data_dictionary

template <>
struct fmt::formatter<data_dictionary::storage_options> {
    constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }
    auto format(const data_dictionary::storage_options&, fmt::format_context& ctx) const -> decltype(ctx.out());
};
