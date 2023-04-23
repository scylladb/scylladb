/*
 * Copyright (C) 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

#include "selector.hh"
#include "types/types.hh"
#include "types/user.hh"

namespace cql3 {

namespace selection {

class field_selector : public selector {
    user_type _type;
    size_t _field;
    shared_ptr<selector> _selected;
public:
    static shared_ptr<factory> new_factory(user_type type, size_t field, shared_ptr<selector::factory> factory) {
        struct field_selector_factory : selector::factory {
            user_type _type;
            size_t _field;
            shared_ptr<selector::factory> _factory;

            field_selector_factory(user_type type, size_t field, shared_ptr<selector::factory> factory)
                    : _type(std::move(type)), _field(field), _factory(std::move(factory)) {
            }

            virtual sstring column_name() const override {
                auto&& name = _type->field_name(_field);
                auto sname = sstring(reinterpret_cast<const char*>(name.begin()), name.size());
                return format("{}.{}", _factory->column_name(), sname);
            }

            virtual data_type get_return_type() const override {
                return _type->field_type(_field);
            }

            shared_ptr<selector> new_instance() const override {
                return make_shared<field_selector>(_type, _field, _factory->new_instance());
            }

            bool is_aggregate_selector_factory() const override {
                return _factory->is_aggregate_selector_factory();
            }
        };
        return make_shared<field_selector_factory>(std::move(type), field, std::move(factory));
    }

    virtual bool is_aggregate() const override {
        return false;
    }

    virtual void add_input(result_set_builder& rs) override {
        _selected->add_input(rs);
    }

    virtual managed_bytes_opt get_output() override {
        auto&& value = _selected->get_output();
        if (!value) {
            return std::nullopt;
        }
        return get_nth_tuple_element(managed_bytes_view(*value), _field);
    }

    virtual data_type get_type() const override {
        return _type->field_type(_field);
    }

    virtual void reset() override {
        _selected->reset();
    }

    virtual sstring assignment_testable_source_context() const override {
        auto&& name = _type->field_name(_field);
        auto sname = std::string_view(reinterpret_cast<const char*>(name.data()), name.size());
        return format("{}.{}", _selected, sname);
    }

    field_selector(user_type type, size_t field, shared_ptr<selector> selected)
            : _type(std::move(type)), _field(field), _selected(std::move(selected)) {
    }
};

}
}
