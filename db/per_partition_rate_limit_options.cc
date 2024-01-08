/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include <optional>
#include <boost/range/adaptor/map.hpp>
#include "exceptions/exceptions.hh"
#include "serializer.hh"
#include "schema/schema.hh"
#include "log.hh"

namespace db {

const char* per_partition_rate_limit_options::max_writes_per_second_key = "max_writes_per_second";
const char* per_partition_rate_limit_options::max_reads_per_second_key = "max_reads_per_second";

per_partition_rate_limit_options::per_partition_rate_limit_options(std::map<sstring, sstring> map) {
    auto handle_uint32_arg = [&] (const char* key) -> std::optional<uint32_t> {
        auto it = map.find(key);
        if (it == map.end()) {
            return std::nullopt;
        }
        try {
            const uint32_t ret = std::stol(it->second);
            map.erase(it);
            return ret;
        } catch (std::invalid_argument&) {
            throw exceptions::configuration_exception(format(
                    "Invalid value for {} option: expected a non-negative number",
                    key));
        } catch (std::out_of_range&) {
            throw exceptions::configuration_exception(format(
                    "Value for {} is out of range accepted by 32-bit numbers",
                    key));
        }
    };

    _max_writes_per_second = handle_uint32_arg(max_writes_per_second_key);
    _max_reads_per_second = handle_uint32_arg(max_reads_per_second_key);

    if (!map.empty()) {
        throw exceptions::configuration_exception(format(
                "Unknown keys in map for per_partition_rate_limit extension: {}",
                fmt::join(map | boost::adaptors::map_keys, ", ")));
    }
}

std::map<sstring, sstring> per_partition_rate_limit_options::to_map() const {
    std::map<sstring, sstring> ret;
    if (_max_writes_per_second) {
        ret.insert_or_assign(max_writes_per_second_key, std::to_string(*_max_writes_per_second));
    }
    if (_max_reads_per_second) {
        ret.insert_or_assign(max_reads_per_second_key, std::to_string(*_max_reads_per_second));
    }
    return ret;
}

}
