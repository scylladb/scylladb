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
#include "cql3/lists.hh"

namespace cql3 {

expr::expression tuples::delayed_value::to_expression() {
    throw std::runtime_error(fmt::format("to_expression not implemented! {}:{}", __FILE__, __LINE__));
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
        return tuples::in_value(elements, type.shared_from_this());
    } catch (marshal_exception& e) {
        throw exceptions::invalid_request_exception(e.what());
    }
}

cql3::raw_value tuples::in_value::get(const query_options& options) {
    const list_type_impl& my_list_type = dynamic_cast<const list_type_impl&>(get_value_type()->without_reversed());
    data_type element_tuple_type = my_list_type.get_elements_type();

    utils::chunked_vector<managed_bytes_opt> list_elements;
    list_elements.reserve(_elements.size());
    for (const std::vector<managed_bytes_opt>& tuple_elements : _elements) {
        ::shared_ptr<tuples::value> tvalue =
            ::make_shared<tuples::value>(tuples::value(tuple_elements, element_tuple_type));

        expr::constant tuple_val = expr::evaluate(tvalue, options);
        list_elements.emplace_back(std::move(tuple_val.value).to_managed_bytes());
    }

    ::shared_ptr<lists::value> list_value = ::make_shared<lists::value>(std::move(list_elements), get_value_type());
    return list_value->get(options);
}

expr::expression tuples::marker::to_expression() {
    throw std::runtime_error(fmt::format("to_expression not implemented! {}:{}", __FILE__, __LINE__));
}

expr::expression tuples::in_marker::to_expression() {
    throw std::runtime_error(fmt::format("to_expression not implemented! {}:{}", __FILE__, __LINE__));
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
