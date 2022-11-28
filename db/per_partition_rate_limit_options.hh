/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <optional>
#include <map>

#include <seastar/core/sstring.hh>

#include "operation_type.hh"

using namespace seastar;

namespace db {

class per_partition_rate_limit_options final {
private:
    static const char* max_writes_per_second_key;
    static const char* max_reads_per_second_key;

private:
    std::optional<uint32_t> _max_writes_per_second;
    std::optional<uint32_t> _max_reads_per_second;

public:
    per_partition_rate_limit_options() = default;
    per_partition_rate_limit_options(std::map<sstring, sstring> map);

    std::map<sstring, sstring> to_map() const;

    inline std::optional<uint32_t> get_max_ops_per_second(operation_type op_type) const {
        switch (op_type) {
        case operation_type::write:
            return _max_writes_per_second;
        case operation_type::read:
            return _max_reads_per_second;
        }
        std::abort(); // compiler will error before we reach here
    }

    inline void set_max_writes_per_second(std::optional<uint32_t> v) {
        _max_writes_per_second = v;
    }

    inline std::optional<uint32_t> get_max_writes_per_second() const {
        return _max_writes_per_second;
    }

    inline void set_max_reads_per_second(std::optional<uint32_t> v) {
        _max_reads_per_second = v;
    }

    inline std::optional<uint32_t> get_max_reads_per_second() const {
        return _max_reads_per_second;
    }
};

}
