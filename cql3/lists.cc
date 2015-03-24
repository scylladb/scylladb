/*
 * Copyright (C) 2015 Cloudius Systems, Ltd.
 */

#include "lists.hh"
#include "update_parameters.hh"
#include "column_identifier.hh"
#include "cql3_type.hh"

namespace cql3 {

shared_ptr<column_specification>
lists::index_spec_of(shared_ptr<column_specification> column) {
    return make_shared<column_specification>(column->ks_name, column->cf_name,
            ::make_shared<column_identifier>(sprint("idx(%s)", *column->name), true), int32_type);
}

shared_ptr<column_specification>
lists::value_spec_of(shared_ptr<column_specification> column) {
    return make_shared<column_specification>(column->ks_name, column->cf_name,
            ::make_shared<column_identifier>(sprint("value(%s)", *column->name), true),
                dynamic_pointer_cast<list_type_impl>(column->type)->get_elements_type());
}

shared_ptr<term>
lists::literal::prepare(const sstring& keyspace, shared_ptr<column_specification> receiver) {
    validate_assignable_to(keyspace, receiver);

    auto&& value_spec = value_spec_of(receiver);
    std::vector<shared_ptr<term>> values;
    values.reserve(_elements.size());
    bool all_terminal = true;
    for (auto rt : _elements) {
        auto&& t = rt->prepare(keyspace, value_spec);

        if (t->contains_bind_marker()) {
            throw exceptions::invalid_request_exception(sprint("Invalid list literal for %s: bind variables are not supported inside collection literals", *receiver->name));
        }
        if (dynamic_pointer_cast<non_terminal>(t)) {
            all_terminal = false;
        }
        values.push_back(std::move(t));
    }
    delayed_value value(values);
    if (all_terminal) {
        return value.bind(query_options::DEFAULT);
    } else {
        return make_shared(std::move(value));
    }
}

void
lists::literal::validate_assignable_to(const sstring keyspace, shared_ptr<column_specification> receiver) {
    if (!dynamic_pointer_cast<list_type_impl>(receiver->type)) {
        throw exceptions::invalid_request_exception(sprint("Invalid list literal for %s of type %s",
                *receiver->name, *receiver->type->as_cql3_type()));
    }
    auto&& value_spec = value_spec_of(receiver);
    for (auto rt : _elements) {
        if (!is_assignable(rt->test_assignment(keyspace, value_spec))) {
            throw exceptions::invalid_request_exception(sprint("Invalid list literal for %s: value %s is not of type %s",
                    *receiver->name, *rt, *value_spec->type->as_cql3_type()));
        }
    }
}

assignment_testable::test_result
lists::literal::test_assignment(const sstring& keyspace, shared_ptr<column_specification> receiver) {
    if (!dynamic_pointer_cast<list_type_impl>(receiver->type)) {
        return assignment_testable::test_result::NOT_ASSIGNABLE;
    }

    // If there is no elements, we can't say it's an exact match (an empty list if fundamentally polymorphic).
    if (_elements.empty()) {
        return assignment_testable::test_result::WEAKLY_ASSIGNABLE;
    }

    auto&& value_spec = value_spec_of(receiver);
    std::vector<shared_ptr<assignment_testable>> to_test;
    to_test.reserve(_elements.size());
    std::copy(_elements.begin(), _elements.end(), std::back_inserter(to_test));
    return assignment_testable::test_all(keyspace, value_spec, to_test);
}

sstring
lists::literal::to_string() const {
    return ::to_string(_elements);
}

lists::value
lists::value::from_serialized(bytes_view v, shared_ptr<list_type_impl> type, serialization_format sf) {
    try {
        // Collections have this small hack that validate cannot be called on a serialized object,
        // but compose does the validation (so we're fine).
        // FIXME: deserializeForNativeProtocol()?!
        auto&& l = boost::any_cast<list_type_impl::native_type>(type->deserialize(v, sf));
        std::vector<bytes> elements;
        elements.reserve(l.size());
        for (auto&& element : l) {
            // elements can be null in lists that represent a set of IN values
            // FIXME: assumes that empty bytes is equivalent to null element
            elements.push_back(element.empty() ? bytes() : type->get_elements_type()->decompose(element));
        }
        return value(std::move(elements));
    } catch (marshal_exception& e) {
        throw exceptions::invalid_request_exception(e.what());
    }
}

bytes_opt
lists::value::get(const query_options& options) {
    return get_with_protocol_version(options.get_serialization_format());
}

bytes
lists::value::get_with_protocol_version(serialization_format sf) {
    return collection_type_impl::pack(_elements.begin(), _elements.end(), _elements.size(), sf);
}

bool
lists::value::equals(shared_ptr<list_type_impl> lt, const value& v) {
    if (_elements.size() != v._elements.size()) {
        return false;
    }
    return std::equal(_elements.begin(), _elements.end(),
            v._elements.begin(),
            [t = lt->get_elements_type()] (bytes_view e1, bytes_view e2) { return t->equal(e1, e2); });
}

std::vector<bytes>
lists::value::get_elements() {
    return _elements;
}

sstring
lists::value::to_string() const {
    std::ostringstream os;
    os << "[";
    bool is_first = true;
    for (auto&& e : _elements) {
        if (!is_first) {
            os << ", ";
        }
        is_first = false;
        os << to_hex(e);
    }
    os << "]";
    return os.str();
}

bool
lists::delayed_value::contains_bind_marker() const {
    // False since we don't support them in collection
    return false;
}

void
lists::delayed_value::collect_marker_specification(shared_ptr<variable_specifications> bound_names) {
}

shared_ptr<terminal>
lists::delayed_value::bind(const query_options& options) {
    std::vector<bytes> buffers;
    buffers.reserve(_elements.size());
    for (auto&& t : _elements) {
        bytes_opt bo = t->bind_and_get(options);

        if (!bo) {
            throw exceptions::invalid_request_exception("null is not supported inside collections");
        }

        // We don't support value > 64K because the serialization format encode the length as an unsigned short.
        if (bo->size() > std::numeric_limits<uint16_t>::max()) {
            throw exceptions::invalid_request_exception(sprint("List value is too long. List values are limited to %d bytes but %d bytes value provided",
                    std::numeric_limits<uint16_t>::max(),
                    bo->size()));
        }

        buffers.push_back(std::move(*bo));
    }
    return ::make_shared<value>(buffers);
}

::shared_ptr<terminal>
lists::marker::bind(const query_options& options) {
    throw std::runtime_error("");
}
#if 0
    public Value bind(QueryOptions options) throws InvalidRequestException
    {
        ByteBuffer value = options.getValues().get(bindIndex);
        return value == null ? null : Value.fromSerialized(value, (ListType)receiver.type, options.getProtocolVersion());
    }
#endif

void
lists::setter::execute(mutation& m, const exploded_clustering_prefix& prefix, const update_parameters& params) {
    tombstone ts;
    if (column.type->is_multi_cell()) {
        // delete + append
        ts = params.make_tombstone_just_before();
    }
    do_append(_t, m, prefix, column, params, ts);
}

bool
lists::setter_by_index::requires_read() {
    return true;
}

void
lists::setter_by_index::collect_marker_specification(shared_ptr<variable_specifications> bound_names) {
    operation::collect_marker_specification(bound_names);
    _idx->collect_marker_specification(std::move(bound_names));
}

void
lists::setter_by_index::execute(mutation& m, const exploded_clustering_prefix& prefix, const update_parameters& params) {
    // we should not get here for frozen lists
    assert(column.type->is_multi_cell()); // "Attempted to set an individual element on a frozen list";

    auto row_key = clustering_key::from_clustering_prefix(*params._schema, prefix);

    bytes_opt index = _idx->bind_and_get(params._options);
    bytes_opt value = _t->bind_and_get(params._options);

    if (!index) {
        throw exceptions::invalid_request_exception("Invalid null value for list index");
    }

    collection_mutation::view existing_list_ser = params.get_prefetched_list(m.key, row_key, column);
    auto ltype = dynamic_pointer_cast<list_type_impl>(column.type);
    collection_type_impl::mutation_view existing_list = ltype->deserialize_mutation_form(existing_list_ser.data);
    // we verified that index is an int32_type
    auto idx = net::ntoh(int32_t(*unaligned_cast<int32_t>(index->begin())));
    if (idx < 0 || size_t(idx) >= existing_list.cells.size()) {
        throw exceptions::invalid_request_exception(sprint("List index %d out of bound, list has size %d",
                idx, existing_list.cells.size()));
    }

    bytes_view eidx = existing_list.cells[idx].first;
    list_type_impl::mutation mut;
    mut.cells.reserve(1);
    if (!value) {
        mut.cells.emplace_back(to_bytes(eidx), params.make_dead_cell());
    } else {
        if (value->size() > std::numeric_limits<uint16_t>::max()) {
            throw exceptions::invalid_request_exception(
                    sprint("List value is too long. List values are limited to %d bytes but %d bytes value provided",
                            std::numeric_limits<uint16_t>::max(), value->size()));
        }
        mut.cells.emplace_back(to_bytes(eidx), params.make_cell(*value));
    }
    auto smut = ltype->serialize_mutation_form(mut);
    m.set_cell(prefix, column, atomic_cell_or_collection::from_collection_mutation(std::move(smut)));
}

void
lists::do_append(shared_ptr<term> t,
        mutation& m,
        const exploded_clustering_prefix& prefix,
        const column_definition& column,
        const update_parameters& params,
        tombstone ts) {
    auto&& value = t->bind(params._options);
    auto&& list_value = dynamic_pointer_cast<lists::value>(value);
    auto&& ltype = dynamic_pointer_cast<list_type_impl>(column.type);
    if (column.type->is_multi_cell()) {
        // If we append null, do nothing. Note that for Setter, we've
        // already removed the previous value so we're good here too
        if (!value) {
            return;
        }

        auto&& to_add = list_value->_elements;
        collection_type_impl::mutation appended;
        appended.tomb = ts;
        appended.cells.reserve(to_add.size());
        for (auto&& e : to_add) {
            auto uuid1 = utils::UUID_gen::get_time_UUID_bytes();
            auto uuid = bytes(reinterpret_cast<const char*>(uuid1.data()), uuid1.size());
            appended.cells.emplace_back(std::move(uuid), params.make_cell(e));
        }
        m.set_cell(prefix, column, ltype->serialize_mutation_form(appended));
    } else {
        // for frozen lists, we're overwriting the whole cell value
        if (!value) {
            m.set_cell(prefix, column, params.make_dead_cell());
        } else {
            auto&& to_add = list_value->_elements;
            auto&& newv = collection_mutation::one{list_type_impl::pack(to_add.begin(), to_add.end(), to_add.size(),
                                                                        serialization_format::internal())};
            m.set_cell(prefix, column, atomic_cell_or_collection::from_collection_mutation(std::move(newv)));
        }
    }
}

}
