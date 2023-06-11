/*
 * Modified by ScyllaDB
 *
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

#include "selector.hh"
#include "selection.hh"

namespace cql3 {

namespace selection {

class writetime_or_ttl_selector : public selector {
    sstring _column_name;
    int _idx;
    bool _is_writetime;
    managed_bytes_opt _current;
public:
    static shared_ptr<selector::factory> new_factory(sstring column_name, int idx, bool is_writetime) {
        class wtots_factory : public selector::factory {
            sstring _column_name;
            int _idx;
            bool _is_writetime;
        public:
            wtots_factory(sstring column_name, int idx, bool is_writetime)
                : _column_name(std::move(column_name)), _idx(idx), _is_writetime(is_writetime) {
            }

            virtual sstring column_name() const override {
                return format("{}({})", _is_writetime ? "writetime" : "ttl", _column_name);
            }

            virtual data_type get_return_type() const override {
                return _is_writetime ? long_type : int32_type;
            }

            virtual shared_ptr<selector> new_instance() const override {
                return ::make_shared<writetime_or_ttl_selector>(_column_name, _idx, _is_writetime);
            }

            virtual bool is_write_time_selector_factory() const override {
                return _is_writetime;
            }

            virtual bool is_ttl_selector_factory() const override {
                return !_is_writetime;
            }
        };
        return ::make_shared<wtots_factory>(std::move(column_name), idx, is_writetime);
    }

    virtual void add_input(result_set_builder& rs) override {
        if (_is_writetime) {
            int64_t ts = rs.timestamp_of(_idx);
            if (ts != api::missing_timestamp) {
                auto tmp = bytes(bytes::initialized_later(), 8);
                auto i = tmp.begin();
                serialize_int64(i, ts);
                _current = managed_bytes(tmp);
            } else {
                _current = std::nullopt;
            }
        } else {
            int ttl = rs.ttl_of(_idx);
            if (ttl > 0) {
                auto tmp = bytes(bytes::initialized_later(), 4);
                auto i = tmp.begin();
                serialize_int32(i, ttl);
                _current = managed_bytes(tmp);
            } else {
                _current = std::nullopt;
            }
        }
    }

    virtual managed_bytes_opt get_output() override {
        return _current;
    }

    virtual void reset() override {
        _current = std::nullopt;
    }

    virtual data_type get_type() const override {
        return _is_writetime ? long_type : int32_type;
    }

#if 0
    @Override
    public String toString()
    {
        return columnName;
    }
#endif

    writetime_or_ttl_selector(sstring column_name, int idx, bool is_writetime)
            : _column_name(std::move(column_name)), _idx(idx), _is_writetime(is_writetime) {
    }
};

}
}
