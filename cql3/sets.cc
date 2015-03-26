/*
 * Copyright (C) 2015 Cloudius Systems, Ltd.
 */

#include "sets.hh"

namespace cql3 {

shared_ptr<column_specification>
sets::value_spec_of(shared_ptr<column_specification> column) {
    return make_shared<column_specification>(column->ks_name, column->cf_name,
            ::make_shared<column_identifier>(sprint("value(%s)", *column->name), true),
            dynamic_pointer_cast<set_type_impl>(column->type)->get_elements_type());
}

shared_ptr<term>
sets::literal::prepare(const sstring& keyspace, shared_ptr<column_specification> receiver) {
    validate_assignable_to(keyspace, receiver);

    // We've parsed empty maps as a set literal to break the ambiguity so
    // handle that case now
    if (_elements.empty() && dynamic_pointer_cast<map_type_impl>(receiver->type)) {
        // use empty_type for comparator, set is empty anyway.
        std::map<bytes, bytes, serialized_compare> m(empty_type->as_less_comparator());
        return ::make_shared<maps::value>(std::move(m));
    }

    auto value_spec = value_spec_of(receiver);
    std::vector<shared_ptr<term>> values;
    values.reserve(_elements.size());
    bool all_terminal = true;
    for (shared_ptr<term::raw> rt : _elements)
    {
        auto t = rt->prepare(keyspace, value_spec);

        if (t->contains_bind_marker()) {
            throw exceptions::invalid_request_exception(sprint("Invalid set literal for %s: bind variables are not supported inside collection literals", *receiver->name));
        }

        if (dynamic_pointer_cast<non_terminal>(t)) {
            all_terminal = false;
        }

        values.push_back(std::move(t));
    }
    auto compare = dynamic_pointer_cast<set_type_impl>(receiver->type)->get_elements_type()->as_less_comparator();

    auto value = ::make_shared<delayed_value>(compare, std::move(values));
    if (all_terminal) {
        return value->bind(query_options::DEFAULT);
    } else {
        return value;
    }
}

void
sets::literal::validate_assignable_to(const sstring& keyspace, shared_ptr<column_specification> receiver) {
    if (!dynamic_pointer_cast<set_type_impl>(receiver->type)) {
        // We've parsed empty maps as a set literal to break the ambiguity so
        // handle that case now
        if (dynamic_pointer_cast<map_type_impl>(receiver->type) && _elements.empty()) {
            return;
        }

        throw exceptions::invalid_request_exception(sprint("Invalid set literal for %s of type %s", *receiver->name, *receiver->type->as_cql3_type()));
    }

    auto&& value_spec = value_spec_of(receiver);
    for (shared_ptr<term::raw> rt : _elements) {
        if (!is_assignable(rt->test_assignment(keyspace, value_spec))) {
            throw exceptions::invalid_request_exception(sprint("Invalid set literal for %s: value %s is not of type %s", *receiver->name, *rt, *value_spec->type->as_cql3_type()));
        }
    }
}

assignment_testable::test_result
sets::literal::test_assignment(const sstring& keyspace, shared_ptr<column_specification> receiver) {
    if (!dynamic_pointer_cast<set_type_impl>(receiver->type)) {
        // We've parsed empty maps as a set literal to break the ambiguity so handle that case now
        if (dynamic_pointer_cast<map_type_impl>(receiver->type) && _elements.empty()) {
            return assignment_testable::test_result::WEAKLY_ASSIGNABLE;
        }

        return assignment_testable::test_result::NOT_ASSIGNABLE;
    }

    // If there is no elements, we can't say it's an exact match (an empty set if fundamentally polymorphic).
    if (_elements.empty()) {
        return assignment_testable::test_result::WEAKLY_ASSIGNABLE;
    }

    auto&& value_spec = value_spec_of(receiver);
    // FIXME: make assignment_testable::test_all() accept ranges
    std::vector<shared_ptr<assignment_testable>> to_test(_elements.begin(), _elements.end());
    return assignment_testable::test_all(keyspace, value_spec, to_test);
}

sstring
sets::literal::to_string() const {
    return "{" + join(", ", _elements) + "}";
}

sets::value
sets::value::from_serialized(bytes_view v, set_type type, serialization_format sf) {
    try {
        // Collections have this small hack that validate cannot be called on a serialized object,
        // but compose does the validation (so we're fine).
        // FIXME: deserializeForNativeProtocol?!
        auto s = boost::any_cast<set_type_impl::native_type>(type->deserialize(v, sf));
        std::set<bytes, serialized_compare> elements(type->as_less_comparator());
        for (auto&& element : s) {
            elements.insert(elements.end(), type->get_elements_type()->decompose(element));
        }
        return value(std::move(elements));
    } catch (marshal_exception& e) {
        throw exceptions::invalid_request_exception(e.why());
    }
}

bytes_opt
sets::value::get(const query_options& options) {
    return get_with_protocol_version(options.get_serialization_format());
}

bytes
sets::value::get_with_protocol_version(serialization_format sf) {
    return collection_type_impl::pack(_elements.begin(), _elements.end(),
            _elements.size(), sf);
}

bool
sets::value::equals(set_type st, const value& v) {
    if (_elements.size() != v._elements.size()) {
        return false;
    }
    auto&& elements_type = st->get_elements_type();
    return std::equal(_elements.begin(), _elements.end(),
            v._elements.begin(),
            [elements_type] (bytes_view v1, bytes_view v2) {
                return elements_type->equal(v1, v2);
            });
}

sstring
sets::value::to_string() const {
    sstring result = "{";
    bool first = true;
    for (auto&& e : _elements) {
        if (!first) {
            result += ", ";
        }
        first = true;
        result += to_hex(e);
    }
    result += "}";
    return result;
}

bool
sets::delayed_value::contains_bind_marker() const {
    // False since we don't support them in collection
    return false;
}

void
sets::delayed_value::collect_marker_specification(shared_ptr<variable_specifications> bound_names) {
}

shared_ptr<terminal>
sets::delayed_value::bind(const query_options& options) {
    std::set<bytes, serialized_compare> buffers(_comparator);
    for (auto&& t : _elements) {
        bytes_opt b = t->bind_and_get(options);

        if (!b) {
            throw exceptions::invalid_request_exception("null is not supported inside collections");
        }

        // We don't support value > 64K because the serialization format encode the length as an unsigned short.
        if (b->size() > std::numeric_limits<uint16_t>::max()) {
            throw exceptions::invalid_request_exception(sprint("Set value is too long. Set values are limited to %d bytes but %d bytes value provided",
                    std::numeric_limits<uint16_t>::max(),
                    b->size()));
        }

        buffers.insert(buffers.end(), std::move(*b));
    }
    return ::make_shared<value>(std::move(buffers));
}


::shared_ptr<terminal>
sets::marker::bind(const query_options& options) {
    throw std::runtime_error("");
}
#if 0
public Value bind(QueryOptions options) throws InvalidRequestException
{
    ByteBuffer value = options.getValues().get(bindIndex);
    return value == null ? null : Value.fromSerialized(value, (SetType)receiver.type, options.getProtocolVersion());
}
#endif

void
sets::setter::execute(mutation& m, const exploded_clustering_prefix& row_key, const update_parameters& params) {
    tombstone ts;
    if (column.type->is_multi_cell()) {
        // delete + add
        ts = params.make_tombstone_just_before();
    }
    adder::do_add(m, row_key, params, _t, column, ts);
}

void
sets::adder::execute(mutation& m, const exploded_clustering_prefix& row_key, const update_parameters& params) {
    assert(column.type->is_multi_cell()); // "Attempted to add items to a frozen set";
    do_add(m, row_key, params, _t, column);
}

void
sets::adder::do_add(mutation& m, const exploded_clustering_prefix& row_key, const update_parameters& params,
        shared_ptr<term> t, const column_definition& column, tombstone ts) {
    auto&& value = t->bind(params._options);
    auto set_value = dynamic_pointer_cast<sets::value>(std::move(value));
    auto set_type = dynamic_pointer_cast<set_type_impl>(column.type);
    if (column.type->is_multi_cell()) {
        // FIXME: mutation_view? not compatible with params.make_cell().
        collection_type_impl::mutation mut;
        mut.tomb = ts;

        if (!set_value || set_value->_elements.empty()) {
            return;
        }

        for (auto&& e : set_value->_elements) {
            mut.cells.emplace_back(e, params.make_cell({}));
        }
        auto smut = set_type->serialize_mutation_form(mut);

        m.set_cell(row_key, column, std::move(smut));
    } else {
        // for frozen sets, we're overwriting the whole cell
        auto v = set_type->serialize_partially_deserialized_form(
                {set_value->_elements.begin(), set_value->_elements.end()},
                serialization_format::internal());
        if (set_value->_elements.empty()) {
            m.set_cell(row_key, column, params.make_dead_cell());
        } else {
            m.set_cell(row_key, column, params.make_cell(std::move(v)));
        }
    }
}

}
