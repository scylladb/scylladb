/*
 * Copyright (C) 2018-present ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include "server.hh"
#include "utils/utf8.hh"
#include "utils/reusable_buffer.hh"
#include "utils/fragmented_temporary_buffer.hh"

namespace cql_transport {

class request_reader {
    fragmented_temporary_buffer::istream _in;
    bytes_ostream* _linearization_buffer;
private:
    struct exception_thrower {
        [[noreturn]] [[gnu::cold]]
        static void throw_out_of_range(size_t attempted_read, size_t actual_left) {
            throw exceptions::protocol_exception(format("truncated frame: expected {:d} bytes, length is {:d}", attempted_read, actual_left));
        };
    };
    static void validate_utf8(sstring_view s) {
        auto error_pos = utils::utf8::validate_with_error_position(to_bytes_view(s));
        if (error_pos) {
            throw exceptions::protocol_exception(format("Cannot decode string as UTF8, invalid character at byte offset {}", *error_pos));
        }
    }

    static db::consistency_level wire_to_consistency(int16_t v) {
        switch (v) {
        case 0x0000: return db::consistency_level::ANY;
        case 0x0001: return db::consistency_level::ONE;
        case 0x0002: return db::consistency_level::TWO;
        case 0x0003: return db::consistency_level::THREE;
        case 0x0004: return db::consistency_level::QUORUM;
        case 0x0005: return db::consistency_level::ALL;
        case 0x0006: return db::consistency_level::LOCAL_QUORUM;
        case 0x0007: return db::consistency_level::EACH_QUORUM;
        case 0x0008: return db::consistency_level::SERIAL;
        case 0x0009: return db::consistency_level::LOCAL_SERIAL;
        case 0x000A: return db::consistency_level::LOCAL_ONE;
        default:     throw exceptions::protocol_exception(format("Unknown code {:d} for a consistency level", v));
        }
    }
public:
    explicit request_reader(fragmented_temporary_buffer::istream in, bytes_ostream& linearization_buffer) noexcept
        : _in(std::move(in))
        , _linearization_buffer(&linearization_buffer)
    { }

    fragmented_temporary_buffer::istream get_stream() {
        return _in;
    }

    size_t bytes_left() const {
        return _in.bytes_left();
    }

    bytes_view read_raw_bytes_view(size_t n) {
        return _in.read_bytes_view(n, *_linearization_buffer, exception_thrower());
    }

    int8_t read_byte() {
        return _in.read<int8_t>(exception_thrower());
    }

    int32_t read_int() {
        return be_to_cpu(_in.read<int32_t>(exception_thrower()));
    }

    int64_t read_long() {
        return be_to_cpu(_in.read<int64_t>(exception_thrower()));
    }

    uint16_t read_short() {
        return be_to_cpu(_in.read<uint16_t>(exception_thrower()));
    }

    sstring read_string() {
        auto n = read_short();
        sstring s = uninitialized_string(n);
        _in.read_to(n, s.begin(), exception_thrower());
        validate_utf8(s);
        return s;
    }

    sstring_view read_string_view() {
        auto n = read_short();
        auto bv = _in.read_bytes_view(n, *_linearization_buffer, exception_thrower());
        auto s = sstring_view(reinterpret_cast<const char*>(bv.data()), bv.size());
        validate_utf8(s);
        return s;
    }

    sstring_view read_long_string_view() {
        auto n = read_int();
        auto bv = _in.read_bytes_view(n, *_linearization_buffer, exception_thrower());
        auto s = sstring_view(reinterpret_cast<const char*>(bv.data()), bv.size());
        validate_utf8(s);
        return s;
    }

    bytes_opt read_bytes() {
        auto len = read_int();
        if (len < 0) {
            return {};
        }
        bytes b(bytes::initialized_later(), len);
        _in.read_to(len, b.begin(), exception_thrower());
        return {std::move(b)};
    }

    bytes read_short_bytes() {
        auto n = read_short();
        bytes b(bytes::initialized_later(), n);
        _in.read_to(n, b.begin(), exception_thrower());
        return b;
    }

    cql3::raw_value_view read_value_view(uint8_t version) {
        auto len = read_int();
        if (len < 0) {
            if (version < 4) {
                return cql3::raw_value_view::make_null();
            }
            if (len == -1) {
                return cql3::raw_value_view::make_null();
            } else if (len == -2) {
                return cql3::raw_value_view::make_unset_value();
            } else {
                throw exceptions::protocol_exception(format("invalid value length: {:d}", len));
            }
        }
        return cql3::raw_value_view::make_value(_in.read_view(len, exception_thrower()));
    }

    void read_name_and_value_list(uint8_t version, std::vector<sstring_view>& names, std::vector<cql3::raw_value_view>& values) {
        uint16_t size = read_short();
        names.reserve(size);
        values.reserve(size);
        for (uint16_t i = 0; i < size; i++) {
            names.emplace_back(read_string_view());
            values.emplace_back(read_value_view(version));
        }
    }

    void read_string_list(std::vector<sstring>& strings) {
        uint16_t size = read_short();
        strings.reserve(size);
        for (uint16_t i = 0; i < size; i++) {
            strings.emplace_back(read_string());
        }
    }

    void read_value_view_list(uint8_t version, std::vector<cql3::raw_value_view>& values) {
        uint16_t size = read_short();
        values.reserve(size);
        for (uint16_t i = 0; i < size; i++) {
            values.emplace_back(read_value_view(version));
        }
    }

    db::consistency_level read_consistency() {
        return wire_to_consistency(read_short());
    }

    std::unordered_map<sstring, sstring> read_string_map() {
        std::unordered_map<sstring, sstring> string_map;
        auto n = read_short();
        for (auto i = 0; i < n; i++) {
            auto key = read_string();
            auto val = read_string();
            string_map.emplace(std::piecewise_construct,
                std::forward_as_tuple(std::move(key)),
                std::forward_as_tuple(std::move(val)));
        }
        return string_map;
    }

private:
    enum class options_flag {
        VALUES,
        SKIP_METADATA,
        PAGE_SIZE,
        PAGING_STATE,
        SERIAL_CONSISTENCY,
        TIMESTAMP,
        NAMES_FOR_VALUES
    };

    using options_flag_enum = super_enum<options_flag,
        options_flag::VALUES,
        options_flag::SKIP_METADATA,
        options_flag::PAGE_SIZE,
        options_flag::PAGING_STATE,
        options_flag::SERIAL_CONSISTENCY,
        options_flag::TIMESTAMP,
        options_flag::NAMES_FOR_VALUES
    >;
public:
    std::unique_ptr<cql3::query_options> read_options(uint8_t version, cql_serialization_format cql_ser_format, const cql3::cql_config& cql_config) {
        auto consistency = read_consistency();
        if (version == 1) {
            return std::make_unique<cql3::query_options>(cql_config, consistency, std::nullopt, std::vector<cql3::raw_value_view>{},
                false, cql3::query_options::specific_options::DEFAULT, cql_ser_format);
        }

        assert(version >= 2);

        auto flags = enum_set<options_flag_enum>::from_mask(read_byte());
        std::vector<cql3::raw_value_view> values;
        std::vector<sstring_view> names;

        if (flags.contains<options_flag::VALUES>()) {
            if (flags.contains<options_flag::NAMES_FOR_VALUES>()) {
                read_name_and_value_list(version, names, values);
            } else {
                read_value_view_list(version, values);
            }
        }

        bool skip_metadata = flags.contains<options_flag::SKIP_METADATA>();
        flags.remove<options_flag::VALUES>();
        flags.remove<options_flag::SKIP_METADATA>();

        std::unique_ptr<cql3::query_options> options;
        if (flags) {
            lw_shared_ptr<service::pager::paging_state> paging_state;
            int32_t page_size = flags.contains<options_flag::PAGE_SIZE>() ? read_int() : -1;
            if (flags.contains<options_flag::PAGING_STATE>()) {
                paging_state = service::pager::paging_state::deserialize(read_bytes());
            }

            db::consistency_level serial_consistency = db::consistency_level::SERIAL;
            if (flags.contains<options_flag::SERIAL_CONSISTENCY>()) {
                serial_consistency = read_consistency();
            }

            api::timestamp_type ts = api::missing_timestamp;
            if (flags.contains<options_flag::TIMESTAMP>()) {
                ts = read_long();
                if (ts < api::min_timestamp || ts > api::max_timestamp) {
                    throw exceptions::protocol_exception(format("Out of bound timestamp, must be in [{:d}, {:d}] (got {:d})",
                        api::min_timestamp, api::max_timestamp, ts));
                }
            }

            std::optional<std::vector<sstring_view>> onames;
            if (!names.empty()) {
                onames = std::move(names);
            }
            options = std::make_unique<cql3::query_options>(cql_config, consistency, std::move(onames), std::move(values), skip_metadata,
                cql3::query_options::specific_options{page_size, std::move(paging_state), serial_consistency, ts},
                cql_ser_format);
        } else {
            options = std::make_unique<cql3::query_options>(cql_config, consistency, std::nullopt, std::move(values), skip_metadata,
                cql3::query_options::specific_options::DEFAULT, cql_ser_format);
        }

        return options;
    }
};

}
