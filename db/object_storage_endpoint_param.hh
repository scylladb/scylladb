/*
 * Copyright (C) 2025-present ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <string>
#include <string_view>
#include <variant>
#include <vector>
#include <compare>
#include <fmt/core.h>
#include "utils/s3/creds.hh"

namespace YAML {
    class Node;
}

namespace db {

class object_storage_endpoint_param {
public:
    struct s3_storage {
        std::string endpoint;
        std::string region;
        std::string iam_role_arn;
        bool legacy_format; // FIXME convert it to bool_class after seastar#3198
        // Set when the entry gives its address in `uri`; the entry is then looked up by this name.
        std::string name;
        // Where a keyspace that names no bucket of its own keeps its data. Optional.
        std::string bucket;

        std::strong_ordering operator<=>(const s3_storage&) const = default;
        std::string to_json_string() const;
        std::string key() const;
    };
    struct gs_storage {
        std::string endpoint; // "default"/empty or an URI
        std::string credentials_file; // optional
        // Set when the entry gives its address in `uri`; the entry is then looked up by this name.
        std::string name;
        // Where a keyspace that names no bucket of its own keeps its data. Optional.
        std::string bucket;

        std::strong_ordering operator<=>(const gs_storage&) const = default;
        std::string to_json_string() const;
        std::string key() const;
    };

    object_storage_endpoint_param();
    object_storage_endpoint_param(const object_storage_endpoint_param&);
    object_storage_endpoint_param(s3_storage);
    object_storage_endpoint_param(std::string endpoint, s3::endpoint_config config);
    object_storage_endpoint_param(gs_storage);

    std::strong_ordering operator<=>(const object_storage_endpoint_param&) const;
    bool operator==(const object_storage_endpoint_param&) const;

    std::string to_json_string() const;
    std::string key() const;
    const std::string& type() const;
    const std::string& bucket() const;

    bool is_s3_storage() const;
    bool is_gs_storage() const;

    bool is_storage_of_type(std::string_view) const;

    const s3_storage& get_s3_storage() const;
    const gs_storage& get_gs_storage() const;

    static object_storage_endpoint_param decode(const YAML::Node&);
    // Decodes a list of entries written as JSON (or YAML). Unlike the scylla.yaml path, which
    // skips an entry it cannot decode, this throws std::invalid_argument for anything that is
    // not a list of valid entries with distinct names.
    static std::vector<object_storage_endpoint_param> parse_list(std::string_view text);
    static const std::string s3_type; // "s3"
    static const std::string gs_type; // "gs"
private:
    friend fmt::formatter<object_storage_endpoint_param>;
    std::variant<s3_storage, gs_storage> _data;
};

std::istream& operator>>(std::istream& is, object_storage_endpoint_param& f);

}

template <>
struct fmt::formatter<db::object_storage_endpoint_param> : fmt::formatter<std::string_view> {
    auto format(const db::object_storage_endpoint_param&, fmt::format_context& ctx) const -> decltype(ctx.out());
};
