/*
 * Copyright (C) 2015-present ScyllaDB
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

#include <seastar/core/shared_ptr.hh>

#include "tuples.hh"
#include "types/list.hh"

namespace cql3 {

lw_shared_ptr<column_specification>
tuples::component_spec_of(const column_specification& column, size_t component) {
    return make_lw_shared<column_specification>(
            column.ks_name,
            column.cf_name,
            ::make_shared<column_identifier>(format("{}[{:d}]", column.name, component), true),
            static_pointer_cast<const tuple_type_impl>(column.type->underlying_type())->type(component));
}

shared_ptr<term>
tuples::literal::prepare(database& db, const sstring& keyspace, lw_shared_ptr<column_specification> receiver) const {
    validate_assignable_to(db, keyspace, *receiver);
    std::vector<shared_ptr<term>> values;
    bool all_terminal = true;
    for (size_t i = 0; i < _elements.size(); ++i) {
        auto&& value = _elements[i]->prepare(db, keyspace, component_spec_of(*receiver, i));
        if (dynamic_pointer_cast<non_terminal>(value)) {
            all_terminal = false;
        }
        values.push_back(std::move(value));
    }
    delayed_value value(values, receiver->type);
    if (all_terminal) {
        return value.bind(query_options::DEFAULT);
    } else {
        return make_shared<delayed_value>(std::move(value));
    }
}

shared_ptr<term>
tuples::literal::prepare(database& db, const sstring& keyspace, const std::vector<lw_shared_ptr<column_specification>>& receivers) const {
    if (_elements.size() != receivers.size()) {
        throw exceptions::invalid_request_exception(format("Expected {:d} elements in value tuple, but got {:d}: {}", receivers.size(), _elements.size(), *this));
    }

    std::vector<shared_ptr<term>> values;
    std::vector<data_type> types;
    bool all_terminal = true;
    for (size_t i = 0; i < _elements.size(); ++i) {
        auto&& t = _elements[i]->prepare(db, keyspace, receivers[i]);
        if (dynamic_pointer_cast<non_terminal>(t)) {
            all_terminal = false;
        }
        values.push_back(t);
        types.push_back(receivers[i]->type);
    }
    delayed_value value(std::move(values), tuple_type_impl::get_instance(std::move(types)));
    if (all_terminal) {
        return value.bind(query_options::DEFAULT);
    } else {
        return make_shared<delayed_value>(std::move(value));
    }
}

delayed_cql_value
tuples::delayed_value::to_delayed_cql_value(cql_serialization_format sf) const {
    std::vector<new_term> new_elements;
    for (const shared_ptr<term>& element : _elements) {
        new_elements.push_back(cql3::to_new_term(element, sf));
    }

    return delayed_cql_value(delayed_tuple_value{
        .elements = std::move(new_elements)
    });
}

ordered_cql_value
tuples::value::to_ordered_cql_value(cql_serialization_format) const {
    std::vector<std::variant<managed_bytes, null_value>> new_elements;
    new_elements.reserve(_elements.size());

    for (const managed_bytes_opt& element : _elements) {
        if (element.has_value()) {
            new_elements.emplace_back(*element);
        } else {
            new_elements.emplace_back(null_value{});
        }
    }

    const abstract_type& not_reversed = _my_type->without_reversed();
    const tuple_type_impl* my_tuple_type = dynamic_cast<const tuple_type_impl*>(&not_reversed);

    if (my_tuple_type == nullptr) {
        throw std::runtime_error("tuple::value has type that is not tuple_type_impl!");
    }

    std::vector<std::optional<data_type>> elements_types;
    elements_types.reserve(_elements.size());
    for (size_t i = 0; i < _elements.size(); i++) {
        if (i < my_tuple_type->all_types().size()) {
            elements_types.emplace_back(std::make_optional(my_tuple_type->all_types()[i]));
        } else {
            elements_types.emplace_back(std::nullopt);
        }
    }

    cql_value cql_val(tuple_value{
        .elements = std::move(new_elements),
        .elements_types = elements_types
    });

    return reverse_if_needed(std::move(cql_val), _my_type->is_reversed());
}

tuples::in_value
tuples::in_value::from_serialized(const raw_value_view& value_view, const list_type_impl& type, const query_options& options) {
    try {
        // Collections have this small hack that validate cannot be called on a serialized object,
        // but the deserialization does the validation (so we're fine).
        auto l = value_view.deserialize<list_type_impl::native_type>(type, options.get_cql_serialization_format());
        auto ttype = dynamic_pointer_cast<const tuple_type_impl>(type.get_elements_type());
        assert(ttype);

        utils::chunked_vector<std::vector<managed_bytes_opt>> elements;
        elements.reserve(l.size());
        for (auto&& e : l) {
            // FIXME: Avoid useless copies.
            elements.emplace_back(ttype->split_fragmented(single_fragmented_view(ttype->decompose(e))));
        }
        return tuples::in_value(elements, ttype);
    } catch (marshal_exception& e) {
        throw exceptions::invalid_request_exception(e.what());
    }
}

ordered_cql_value
tuples::in_value::to_ordered_cql_value(cql_serialization_format) const {
    utils::chunked_vector<std::variant<managed_bytes, null_value>> new_elements;
    new_elements.reserve(_elements.size());

    for (const std::vector<managed_bytes_opt>& element : _elements) {
        managed_bytes serialized_element = tuple_type_impl::build_value_fragmented(element);
        new_elements.emplace_back(std::move(serialized_element));
    }

    return ordered_cql_value(cql_value(list_value{
        .elements = std::move(new_elements),
        .elements_type = _elements_type
    }));
}

lw_shared_ptr<column_specification>
tuples::in_raw::make_in_receiver(const std::vector<lw_shared_ptr<column_specification>>& receivers) {
    std::vector<data_type> types;
    types.reserve(receivers.size());
    sstring in_name = "in(";
    for (auto&& receiver : receivers) {
        in_name += receiver->name->text();
        if (receiver != receivers.back()) {
            in_name += ",";
        }

        if (receiver->type->is_collection() && receiver->type->is_multi_cell()) {
            throw exceptions::invalid_request_exception("Non-frozen collection columns do not support IN relations");
        }

        types.emplace_back(receiver->type);
    }
    in_name += ")";

    auto identifier = ::make_shared<column_identifier>(in_name, true);
    auto type = tuple_type_impl::get_instance(types);
    return make_lw_shared<column_specification>(receivers.front()->ks_name, receivers.front()->cf_name, identifier, list_type_impl::get_instance(type, false));
}

tuples::in_marker::in_marker(int32_t bind_index, lw_shared_ptr<column_specification> receiver)
    : abstract_marker(bind_index, std::move(receiver))
{
    assert(dynamic_pointer_cast<const list_type_impl>(_receiver->type));
}

shared_ptr<terminal> tuples::in_marker::bind(const query_options& options) {
    const auto& value = options.get_value_at(_bind_index);
    if (value.is_null()) {
        return nullptr;
    } else if (value.is_unset_value()) {
        throw exceptions::invalid_request_exception(format("Invalid unset value for tuple {}", _receiver->name->text()));
    } else {
        auto& type = static_cast<const list_type_impl&>(*_receiver->type);
        auto& elem_type = static_cast<const tuple_type_impl&>(*type.get_elements_type());
        try {
            auto l = value.validate_and_deserialize<list_type_impl::native_type>(type, options.get_cql_serialization_format());
            for (auto&& element : l) {
                elem_type.validate(elem_type.decompose(element), options.get_cql_serialization_format());
            }
        } catch (marshal_exception& e) {
            throw exceptions::invalid_request_exception(e.what());
        }
        return make_shared<tuples::in_value>(tuples::in_value::from_serialized(value, type, options));
    }
}

}
