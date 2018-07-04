/*
 * Copyright (C) 2018 ScyllaDB
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

#include <boost/locale/encoding_utf.hpp>

#include "server.hh"
#include "utils/reusable_buffer.hh"

namespace cql_transport {

class request_reader {
    bytes_view _in;
private:
    void check_room(size_t n) {
        if (_in.size() < n) {
            throw exceptions::protocol_exception(sprint("truncated frame: expected %lu bytes, length is %lu", n, _in.size()));
        }
    }
    static void validate_utf8(sstring_view s) {
        try {
            boost::locale::conv::utf_to_utf<char>(s.begin(), s.end(), boost::locale::conv::stop);
        } catch (const boost::locale::conv::conversion_error& ex) {
            throw exceptions::protocol_exception("Cannot decode string as UTF8");
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
        default:     throw exceptions::protocol_exception(sprint("Unknown code %d for a consistency level", v));
        }
    }
public:
    explicit request_reader(bytes_view bv) noexcept : _in(bv) { }

    size_t bytes_left() const {
        return _in.size();
    }

    bytes_view read_raw_bytes_view(size_t n) {
        auto bv = bytes_view(_in.data(), n);
        _in.remove_prefix(n);
        return bv;
    }

    int8_t read_byte() {
        check_room(1);
        int8_t n = _in[0];
        _in.remove_prefix(1);
        return n;
    }

    int32_t read_int() {
        check_room(sizeof(int32_t));
        auto p = reinterpret_cast<const uint8_t*>(_in.begin());
        uint32_t n = (static_cast<uint32_t>(p[0]) << 24)
                | (static_cast<uint32_t>(p[1]) << 16)
                | (static_cast<uint32_t>(p[2]) << 8)
                | (static_cast<uint32_t>(p[3]));
        _in.remove_prefix(4);
        return n;
    }

    int64_t read_long() {
        check_room(sizeof(int64_t));
        auto p = reinterpret_cast<const uint8_t*>(_in.begin());
        uint64_t n = (static_cast<uint64_t>(p[0]) << 56)
                | (static_cast<uint64_t>(p[1]) << 48)
                | (static_cast<uint64_t>(p[2]) << 40)
                | (static_cast<uint64_t>(p[3]) << 32)
                | (static_cast<uint64_t>(p[4]) << 24)
                | (static_cast<uint64_t>(p[5]) << 16)
                | (static_cast<uint64_t>(p[6]) << 8)
                | (static_cast<uint64_t>(p[7]));
        _in.remove_prefix(8);
        return n;
    }

    uint16_t read_short() {
        check_room(sizeof(uint16_t));
        auto p = reinterpret_cast<const uint8_t*>(_in.begin());
        uint16_t n = (static_cast<uint16_t>(p[0]) << 8)
                | (static_cast<uint16_t>(p[1]));
        _in.remove_prefix(2);
        return n;
    }

    sstring read_string() {
        auto n = read_short();
        check_room(n);
        sstring s{reinterpret_cast<const char*>(_in.begin()), static_cast<size_t>(n)};
        assert(n >= 0);
        _in.remove_prefix(n);
        validate_utf8(s);
        return s;
    }

    sstring_view read_string_view() {
        auto n = read_short();
        check_room(n);
        sstring_view s{reinterpret_cast<const char*>(_in.begin()), static_cast<size_t>(n)};
        assert(n >= 0);
        _in.remove_prefix(n);
        validate_utf8(s);
        return s;
    }

    sstring_view read_long_string_view() {
        auto n = read_int();
        check_room(n);
        sstring_view s{reinterpret_cast<const char*>(_in.begin()), static_cast<size_t>(n)};
        _in.remove_prefix(n);
        validate_utf8(s);
        return s;
    }

    bytes_opt read_bytes() {
        auto len = read_int();
        if (len < 0) {
            return {};
        }
        check_room(len);
        bytes b(reinterpret_cast<const int8_t*>(_in.begin()), len);
        _in.remove_prefix(len);
        return {std::move(b)};
    }

    bytes read_short_bytes() {
        auto n = read_short();
        check_room(n);
        bytes s{reinterpret_cast<const int8_t*>(_in.begin()), static_cast<size_t>(n)};
        assert(n >= 0);
        _in.remove_prefix(n);
        return s;
    }

    cql3::raw_value read_value(uint8_t version) {
        auto len = read_int();
        if (len < 0) {
            if (version < 4) {
                return cql3::raw_value::make_null();
            }
            if (len == -1) {
                return cql3::raw_value::make_null();
            } else if (len == -2) {
                return cql3::raw_value::make_unset_value();
            } else {
                throw exceptions::protocol_exception(sprint("invalid value length: %d", len));
            }
        }
        check_room(len);
        bytes b(reinterpret_cast<const int8_t*>(_in.begin()), len);
        _in.remove_prefix(len);
        return cql3::raw_value::make_value(std::move(b));
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
                throw exceptions::protocol_exception(sprint("invalid value length: %d", len));
            }
        }
        check_room(len);
        bytes_view bv(reinterpret_cast<const int8_t*>(_in.begin()), len);
        _in.remove_prefix(len);
        return cql3::raw_value_view::make_value(std::move(bv));
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
    std::unique_ptr<cql3::query_options> read_options(uint8_t version, cql_serialization_format cql_ser_format, const timeout_config& timeouts) {
        auto consistency = read_consistency();
        if (version == 1) {
            return std::make_unique<cql3::query_options>(consistency, timeouts, std::experimental::nullopt, std::vector<cql3::raw_value_view>{},
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
            ::shared_ptr<service::pager::paging_state> paging_state;
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
                    throw exceptions::protocol_exception(sprint("Out of bound timestamp, must be in [%d, %d] (got %d)",
                        api::min_timestamp, api::max_timestamp, ts));
                }
            }

            std::experimental::optional<std::vector<sstring_view>> onames;
            if (!names.empty()) {
                onames = std::move(names);
            }
            options = std::make_unique<cql3::query_options>(consistency, timeouts, std::move(onames), std::move(values), skip_metadata,
                cql3::query_options::specific_options{page_size, std::move(paging_state), serial_consistency, ts},
                cql_ser_format);
        } else {
            options = std::make_unique<cql3::query_options>(consistency, timeouts, std::experimental::nullopt, std::move(values), skip_metadata,
                cql3::query_options::specific_options::DEFAULT, cql_ser_format);
        }

        return options;
    }
};

}
