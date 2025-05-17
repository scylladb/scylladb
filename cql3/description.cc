/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "cql3/description.hh"

#include <seastar/core/on_internal_error.hh>

#include "cql3/util.hh"
#include "utils/log.hh"
#include "types/types.hh"

static logging::logger dlogger{"description"};

namespace cql3 {

namespace {

template <typename Desc>
std::vector<managed_bytes_opt> serialize_description(Desc&& desc, bool serialize_create_statement) {
    std::vector<managed_bytes_opt> result{};
    result.reserve(serialize_create_statement ? 4 : 3);

    if (desc.keyspace) {
        result.push_back(to_managed_bytes(cql3::util::maybe_quote(*desc.keyspace)));
    } else {
        result.push_back(to_managed_bytes_opt(data_value::make_null(utf8_type).serialize()));
    }

    result.push_back(to_managed_bytes(desc.type));
    result.push_back(to_managed_bytes(cql3::util::maybe_quote(desc.name)));

    if (serialize_create_statement && !desc.create_statement) {
        on_internal_error(dlogger, "create_statement field is empty");
    }

    if (serialize_create_statement) {
        // We do this to optionally move this field. It may be quite big,
        // so it's worth a little optimization.
        //
        // Note: The standard specifies that the const-qualifier of the result of the expression within
        // `result.push_back(...)` will align with the const-qualifier of the passed `desc`, cf.
        // https://en.cppreference.com/w/cpp/language/operator_member_access.
        result.push_back(std::forward<Desc>(desc).create_statement);
    }

    return result;
}

} // anonymous namespace

std::vector<managed_bytes_opt> description::serialize(bool serialize_create_statement) const {
    return serialize_description(*this, serialize_create_statement);
}

std::vector<managed_bytes_opt> description::serialize(bool serialize_create_statement) && {
    return serialize_description(*this, serialize_create_statement);
}

sstring description::deserialize_create_statement() const {
    SCYLLA_ASSERT(create_statement.has_value());

    auto bytes = to_bytes(*create_statement);
    return sstring(to_string_view(bytes));
}

} // namespace cql3
