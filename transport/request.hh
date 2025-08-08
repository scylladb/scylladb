/*
 * Copyright (C) 2018-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include "server.hh"
#include "utils/utf8.hh"
#include "utils/fragmented_temporary_buffer.hh"
#include "utils/result.hh"

namespace cql_transport {

struct unset_tag {};

struct value_view_and_unset {
    cql3::raw_value_view value;
    bool unset = false;

    value_view_and_unset(cql3::raw_value_view value) : value(std::move(value)) {}
    value_view_and_unset(unset_tag) : value(cql3::raw_value_view::make_null()), unset(true) {}
};

class request_reader {
    fragmented_temporary_buffer::istream _in;
    bytes_ostream* _linearization_buffer;
private:
    struct exception_creator {
        [[gnu::cold]]
        static utils::result_with_eptr<void> out_of_range(size_t attempted_read, size_t actual_left) {
            return bo::failure(std::make_exception_ptr(exceptions::protocol_exception(format("truncated frame: expected {:d} bytes, length is {:d}", attempted_read, actual_left))));
        }
    };
    static utils::result_with_eptr<void> validate_utf8(std::string_view s) {
        auto error_pos = utils::utf8::validate_with_error_position(to_bytes_view(s));
        if (error_pos) {
            return bo::failure(exceptions::protocol_exception(format("Cannot decode string as UTF8, invalid character at byte offset {}", *error_pos)));
        }
        return bo::success();
    }

    static utils::result_with_eptr<db::consistency_level> wire_to_consistency(int16_t v) {
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
        default:     return bo::failure(exceptions::protocol_exception(format("Unknown code {:d} for a consistency level", v)));
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

    utils::result_with_eptr<bytes_view> read_raw_bytes_view(size_t n) {
        utils::result_with_eptr<bytes_view> bv = _in.read_bytes_view(n, *_linearization_buffer, exception_creator());
        if (!bv) [[unlikely]] {
            return bo::failure(std::move(bv).error());
        }
        return std::move(bv).value();
    }

    utils::result_with_eptr<int8_t> read_byte() {
        utils::result_with_eptr<int8_t> v = _in.read<int8_t>(exception_creator());
        if (!v) [[unlikely]] {
            return bo::failure(std::move(v).error());
        }
        return be_to_cpu(std::move(v).value());
    }

    utils::result_with_eptr<int32_t> read_int() {
        utils::result_with_eptr<int32_t> v = _in.read<int32_t>(exception_creator());
        if (!v) [[unlikely]] {
            return bo::failure(std::move(v).error());
        }
        return be_to_cpu(std::move(v).value());
    }

    utils::result_with_eptr<int64_t> read_long() {
        utils::result_with_eptr<int64_t> v = _in.read<int64_t>(exception_creator());
        if (!v) [[unlikely]] {
            return bo::failure(std::move(v).error());
        }
        return be_to_cpu(std::move(v).value());
    }

    utils::result_with_eptr<uint16_t> read_short() {
        utils::result_with_eptr<uint16_t> v = _in.read<uint16_t>(exception_creator());
        if (!v) [[unlikely]] {
            return bo::failure(std::move(v).error());
        }
        return be_to_cpu(v.value());
    }

    utils::result_with_eptr<sstring> read_string() {
        utils::result_with_eptr<uint16_t> n = read_short();
        if (!n) [[unlikely]] {
            return bo::failure(std::move(n).error());
        }
        sstring s = uninitialized_string(n.value());
        auto output = _in.read_to(n.value(), s.begin(), exception_creator());
        if (!output) [[unlikely]] {
            return bo::failure(std::move(output).error());
        }
        utils::result_with_eptr<void> check = validate_utf8(s);
        if (!check) [[unlikely]] {
            return bo::failure(std::move(check).error());
        }
        return s;
    }

    utils::result_with_eptr<std::string_view> read_string_view() {
        utils::result_with_eptr<uint16_t> n = read_short();
        if (!n) [[unlikely]] {
            return bo::failure(std::move(n).error());
        }
        utils::result_with_eptr<bytes_view> bv = _in.read_bytes_view(n.value(), *_linearization_buffer, exception_creator());
        if (!bv) [[unlikely]] {
            return bo::failure(std::move(bv).error());
        }
        auto s = std::string_view(reinterpret_cast<const char*>(bv.value().data()), bv.value().size());
        utils::result_with_eptr<void> check = validate_utf8(s);
        if (!check) [[unlikely]] {
            return bo::failure(std::move(check).error());
        }
        return s;
    }

    utils::result_with_eptr<std::string_view> read_long_string_view() {
        utils::result_with_eptr<int32_t> n = read_int();
        if (!n) [[unlikely]] {
            return bo::failure(std::move(n).error());
        }
        utils::result_with_eptr<bytes_view> bv = _in.read_bytes_view(n.value(), *_linearization_buffer, exception_creator());
        if (!bv) [[unlikely]] {
            return bo::failure(std::move(bv).error());
        }
        auto s = std::string_view(reinterpret_cast<const char*>(bv.value().data()), bv.value().size());
        utils::result_with_eptr<void> check = validate_utf8(s);
        if (!check) [[unlikely]] {
            return bo::failure(std::move(check).error());
        }
        return s;
    }

    utils::result_with_eptr<bytes> read_bytes() {
        utils::result_with_eptr<int32_t> len = read_int();
        if (!len) [[unlikely]] {
            return bo::failure(std::move(len).error());
        }
        if (len.value() < 0) {
            return {bytes(bytes::initialized_later(), 0)};
        }
        bytes b(bytes::initialized_later(), len.value());
        auto output = _in.read_to(len.value(), b.begin(), exception_creator());
        if (!output) [[unlikely]] {
            return bo::failure(std::move(output).error());
        }
        return {std::move(b)};
    }

    utils::result_with_eptr<bytes> read_short_bytes() {
        utils::result_with_eptr<uint16_t> n = read_short();
        if (!n) [[unlikely]] {
            return bo::failure(std::move(n).error());
        }
        bytes b(bytes::initialized_later(), n.value());
        auto output = _in.read_to(n.value(), b.begin(), exception_creator());
        if (!output) [[unlikely]] {
            return bo::failure(std::move(output).error());
        }
        return b;
    }

    utils::result_with_eptr<value_view_and_unset> read_value_view(uint8_t version) {
        utils::result_with_eptr<int32_t> len = read_int();
        if (!len) [[unlikely]] {
            return bo::failure(std::move(len).error());
        }
        if (len.value() < 0) {
            if (version < 4) {
                return bo::success(cql3::raw_value_view::make_null());
            }
            if (len.value() == -1) {
                return bo::success(cql3::raw_value_view::make_null());
            } else if (len.value() == -2) {
                return bo::success(value_view_and_unset(unset_tag()));
            } else {
                return std::make_exception_ptr(exceptions::protocol_exception(format("invalid value length: {:d}", len.value())));
            }
        }
        utils::result_with_eptr<fragmented_temporary_buffer::view> view = _in.read_view(len.value(), exception_creator());
        if (!view) [[unlikely]] {
                return bo::failure(std::move(view).error());
        }
        return bo::success(cql3::raw_value_view::make_value(std::move(view).value()));
    }

    utils::result_with_eptr<void> read_name_and_value_list(uint8_t version, std::vector<std::string_view>& names, std::vector<cql3::raw_value_view>& values,
            cql3::unset_bind_variable_vector& unset) {
        utils::result_with_eptr<uint16_t> size = read_short();
        if (!size) [[unlikely]] {
            return bo::failure(std::move(size).error());
        }
        names.reserve(size.value());
        unset.reserve(size.value());
        values.reserve(size.value());
        for (uint16_t i = 0; i < size.value(); i++) {
            auto name = read_string_view();
            if (!name) [[unlikely]] {
                return bo::failure(std::move(name).error());
            }
            names.emplace_back(std::move(name).value());
            utils::result_with_eptr<value_view_and_unset> vv = read_value_view(version);
            if (!vv) [[unlikely]] {
                return bo::failure(std::move(vv).error());
            }
            auto&& [value, is_unset] = std::move(vv).value();
            values.emplace_back(std::move(value));
            unset.emplace_back(is_unset);
        }
        return bo::success();
    }

    utils::result_with_eptr<void> read_string_list(std::vector<sstring>& strings) {
        utils::result_with_eptr<uint16_t> size = read_short();
        if (!size) [[unlikely]] {
            return bo::failure(std::move(size).error());
        }
        strings.reserve(size.value());
        for (uint16_t i = 0; i < size.value(); i++) {
            utils::result_with_eptr<sstring> str = read_string();
            if (!str) [[unlikely]] {
                return bo::failure(std::move(str).error());
            }
            strings.emplace_back(std::move(str).value());
        }
        return bo::success();
    }

    utils::result_with_eptr<void> read_value_view_list(uint8_t version, std::vector<cql3::raw_value_view>& values, cql3::unset_bind_variable_vector& unset) {
        utils::result_with_eptr<uint16_t> size = read_short();
        if (!size) [[unlikely]] {
            return bo::failure(std::move(size).error());
        }
        values.reserve(size.value());
        unset.reserve(size.value());
        for (uint16_t i = 0; i < size.value(); i++) {
            utils::result_with_eptr<value_view_and_unset> vv = read_value_view(version);
            if (!vv) [[unlikely]] {
                return bo::failure(std::move(vv).error());
            }
            auto&& [value, is_unset] = std::move(vv).value();
            values.emplace_back(std::move(value));
            unset.emplace_back(is_unset);
        }
        return bo::success();
    }

    utils::result_with_eptr<db::consistency_level> read_consistency() {
        utils::result_with_eptr<uint16_t> v = read_short();
        if (!v) [[unlikely]] {
            return bo::failure(std::move(v).error());
        }
        return wire_to_consistency(v.value());
    }

    utils::result_with_eptr<std::unordered_map<sstring, sstring>> read_string_map() {
        std::unordered_map<sstring, sstring> string_map;
        utils::result_with_eptr<uint16_t> n = read_short();
        if (!n) [[unlikely]] {
            return bo::failure(std::move(n).error());
        }
        for (auto i = 0; i < n.value(); i++) {
            utils::result_with_eptr<sstring> key = read_string();
            if (!key) [[unlikely]] {
                return bo::failure(std::move(key).error());
            }
            utils::result_with_eptr<sstring> val = read_string();
            if (!val) [[unlikely]] {
                return bo::failure(std::move(val).error());
            }
            string_map.emplace(std::piecewise_construct,
                std::forward_as_tuple(std::move(key).value()),
                std::forward_as_tuple(std::move(val).value()));
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
    utils::result_with_eptr<std::unique_ptr<cql3::query_options>> read_options(uint8_t version, const cql3::cql_config& cql_config) {
        utils::result_with_eptr<db::consistency_level> consistency = read_consistency();
        if (!consistency) [[unlikely]] {
            return bo::failure(std::move(consistency).error());
        }
        utils::result_with_eptr<int8_t> b = read_byte();
        if (!b) [[unlikely]] {
            return bo::failure(std::move(b).error());
        }
        auto flags = enum_set<options_flag_enum>::from_mask(b.value());
        std::vector<cql3::raw_value_view> values;
        cql3::unset_bind_variable_vector unset;
        std::vector<std::string_view> names;

        if (flags.contains<options_flag::VALUES>()) {
            if (flags.contains<options_flag::NAMES_FOR_VALUES>()) {
                utils::result_with_eptr<void> nvl = read_name_and_value_list(version, names, values, unset);
                if (!nvl) [[unlikely]] {
                    return bo::failure(std::move(nvl).error());
                }
            } else {
                utils::result_with_eptr<void> vvl = read_value_view_list(version, values, unset);
                if (!vvl) [[unlikely]] {
                    return bo::failure(std::move(vvl).error());
                }
            }
        }

        bool skip_metadata = flags.contains<options_flag::SKIP_METADATA>();
        flags.remove<options_flag::VALUES>();
        flags.remove<options_flag::SKIP_METADATA>();

        std::unique_ptr<cql3::query_options> options;
        if (flags) {
            lw_shared_ptr<service::pager::paging_state> paging_state;
            int32_t page_size = -1;
            if (flags.contains<options_flag::PAGE_SIZE>()) {
                utils::result_with_eptr<int32_t> v = read_int();
                if (!v) [[unlikely]] {
                    return bo::failure(std::move(v).error());
                }
                page_size = v.value();
            }
            if (flags.contains<options_flag::PAGING_STATE>()) {
                utils::result_with_eptr<bytes> bv = read_bytes();
                if (!bv) [[unlikely]] {
                    return bo::failure(std::move(bv).error());
                }
                paging_state = service::pager::paging_state::deserialize(bv.value());
            }

            db::consistency_level serial_consistency = db::consistency_level::SERIAL;
            if (flags.contains<options_flag::SERIAL_CONSISTENCY>()) {
                auto sc = read_consistency();
                if (!sc) [[unlikely]] {
                    return bo::failure(std::move(sc).error());
                }
                serial_consistency = sc.value();
            }

            api::timestamp_type ts = api::missing_timestamp;
            if (flags.contains<options_flag::TIMESTAMP>()) {
                utils::result_with_eptr<int64_t> v = read_long();
                if (!v) [[unlikely]] {
                    return bo::failure(std::move(v).error());
                }
                ts = v.value();
                if (ts < api::min_timestamp || ts > api::max_timestamp) {
                    return bo::failure(std::make_exception_ptr(exceptions::protocol_exception(format("Out of bound timestamp, must be in [{:d}, {:d}] (got {:d})",
                        api::min_timestamp, api::max_timestamp, ts))));
                }
            }

            std::optional<std::vector<std::string_view>> onames;
            if (!names.empty()) {
                onames = std::move(names);
            }
            options = std::make_unique<cql3::query_options>(cql_config, consistency.value(), std::move(onames),
                cql3::raw_value_view_vector_with_unset(std::move(values), std::move(unset)), skip_metadata,
                cql3::query_options::specific_options{page_size, std::move(paging_state), serial_consistency, ts});
        } else {
            options = std::make_unique<cql3::query_options>(cql_config, consistency.value(), std::nullopt,
                cql3::raw_value_view_vector_with_unset(std::move(values), std::move(unset)), skip_metadata,
                cql3::query_options::specific_options::DEFAULT);
        }

        return options;
    }
};

}
