/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */
#include <utility>

#include "operation.hh"
#include "operation_impl.hh"
#include "maps.hh"
#include "sets.hh"
#include "lists.hh"
#include "user_types.hh"
#include "cql3/expr/evaluate.hh"
#include "cql3/expr/expr-utils.hh"
#include "types/tuple.hh"
#include "types/map.hh"
#include "types/list.hh"
#include "types/set.hh"
#include "types/user.hh"
#include "service/broadcast_tables/experimental/lang.hh"

namespace cql3 {

void
operation::fill_prepare_context(prepare_context& ctx) {
    if (_e.has_value()) {
        expr::fill_prepare_context(*_e, ctx);
    }
}

sstring
operation::set_element::to_string(const column_definition& receiver) const {
    return format("{}[{}] = {}", receiver.name_as_text(), _selector, _value);
}

shared_ptr<operation>
operation::set_element::prepare(data_dictionary::database db, const sstring& keyspace, const column_definition& receiver) const {
    using exceptions::invalid_request_exception;
    auto rtype = dynamic_pointer_cast<const collection_type_impl>(receiver.type);
    if (!rtype) {
        throw invalid_request_exception(format("Invalid operation ({}) for non collection column {}", to_string(receiver), receiver.name_as_text()));
    } else if (!rtype->is_multi_cell()) {
        throw invalid_request_exception(format("Invalid operation ({}) for frozen collection column {}", to_string(receiver), receiver.name_as_text()));
    }

    if (rtype->get_kind() == abstract_type::kind::list) {
        auto&& lval = prepare_expression(_value, db, keyspace, nullptr, lists::value_spec_of(*receiver.column_specification));
        verify_no_aggregate_functions(lval, "SET clause");
        if (_by_uuid) {
            auto&& idx = prepare_expression(_selector, db, keyspace, nullptr, lists::uuid_index_spec_of(*receiver.column_specification));
            verify_no_aggregate_functions(idx, "SET clause");
            return make_shared<lists::setter_by_uuid>(receiver, std::move(idx), std::move(lval));
        } else {
            auto&& idx = prepare_expression(_selector, db, keyspace, nullptr, lists::index_spec_of(*receiver.column_specification));
            verify_no_aggregate_functions(idx, "SET clause");
            return make_shared<lists::setter_by_index>(receiver, std::move(idx), std::move(lval));
        }
    } else if (rtype->get_kind() == abstract_type::kind::set) {
        throw invalid_request_exception(format("Invalid operation ({}) for set column {}", to_string(receiver), receiver.name_as_text()));
    } else if (rtype->get_kind() == abstract_type::kind::map) {
        auto key = prepare_expression(_selector, db, keyspace, nullptr, maps::key_spec_of(*receiver.column_specification));
        verify_no_aggregate_functions(key, "SET clause");
        auto mval = prepare_expression(_value, db, keyspace, nullptr, maps::value_spec_of(*receiver.column_specification));
        verify_no_aggregate_functions(mval, "SET clause");
        return make_shared<maps::setter_by_key>(receiver, std::move(key), std::move(mval));
    }
    abort();
}

bool
operation::set_element::is_compatible_with(const std::unique_ptr<raw_update>& other) const {
    // TODO: we could check that the other operation is not setting the same element
    // too (but since the index/key set may be a bind variables we can't always do it at this point)
    return !dynamic_cast<const set_value*>(other.get());
}

sstring
operation::set_field::to_string(const column_definition& receiver) const {
    return format("{}.{} = {}", receiver.name_as_text(), *_field, _value);
}

shared_ptr<operation>
operation::set_field::prepare(data_dictionary::database db, const sstring& keyspace, const column_definition& receiver) const {
    if (!receiver.type->is_user_type()) {
        throw exceptions::invalid_request_exception(
                format("Invalid operation ({}) for non-UDT column {}", to_string(receiver), receiver.name_as_text()));
    } else if (!receiver.type->is_multi_cell()) {
        throw exceptions::invalid_request_exception(
                format("Invalid operation ({}) for frozen UDT column {}", to_string(receiver), receiver.name_as_text()));
    }

    auto& type = static_cast<const user_type_impl&>(*receiver.type);
    auto idx = type.idx_of_field(_field->name());
    if (!idx) {
        throw exceptions::invalid_request_exception(
                format("UDT column {} does not have a field named {}", receiver.name_as_text(), *_field));
    }

    auto val = prepare_expression(_value, db, keyspace, nullptr, user_types::field_spec_of(*receiver.column_specification, *idx));
    verify_no_aggregate_functions(val, "SET clause");
    return make_shared<user_types::setter_by_field>(receiver, *idx, std::move(val));
}

bool
operation::set_field::is_compatible_with(const std::unique_ptr<raw_update>& other) const {
    auto x = dynamic_cast<const set_field*>(other.get());
    if (x) {
        return _field != x->_field;
    }

    return !dynamic_cast<const set_value*>(other.get());
}

const column_identifier::raw&
operation::field_deletion::affected_column() const {
    return *_id;
}

shared_ptr<operation>
operation::field_deletion::prepare(data_dictionary::database db, const sstring& keyspace, const column_definition& receiver) const {
    if (!receiver.type->is_user_type()) {
        throw exceptions::invalid_request_exception(
                format("Invalid deletion operation for non-UDT column {}", receiver.name_as_text()));
    } else if (!receiver.type->is_multi_cell()) {
        throw exceptions::invalid_request_exception(
                format("Frozen UDT column {} does not support field deletions", receiver.name_as_text()));
    }

    auto type = static_cast<const user_type_impl*>(receiver.type.get());
    auto idx = type->idx_of_field(_field->name());
    if (!idx) {
        throw exceptions::invalid_request_exception(
                format("UDT column {} does not have a field named {}", receiver.name_as_text(), *_field));
    }

    return make_shared<user_types::deleter_by_field>(receiver, *idx);
}

sstring
operation::addition::to_string(const column_definition& receiver) const {
    return format("{} = {} + {}", receiver.name_as_text(), receiver.name_as_text(), _value);
}

shared_ptr<operation>
operation::addition::prepare(data_dictionary::database db, const sstring& keyspace, const column_definition& receiver) const {
    auto v = prepare_expression(_value, db, keyspace, nullptr, receiver.column_specification);
    verify_no_aggregate_functions(v, "SET clause");

    auto ctype = dynamic_pointer_cast<const collection_type_impl>(receiver.type);
    if (!ctype) {
        if (!receiver.is_counter()) {
            throw exceptions::invalid_request_exception(format("Invalid operation ({}) for non counter column {}", to_string(receiver), receiver.name_as_text()));
        }
        return make_shared<constants::adder>(receiver, std::move(v));
    } else if (!ctype->is_multi_cell()) {
        throw exceptions::invalid_request_exception(format("Invalid operation ({}) for frozen collection column {}", to_string(receiver), receiver.name_as_text()));
    }

    if (ctype->get_kind() == abstract_type::kind::list) {
        return make_shared<lists::appender>(receiver, std::move(v));
    } else if (ctype->get_kind() == abstract_type::kind::set) {
        return make_shared<sets::adder>(receiver, std::move(v));
    } else if (ctype->get_kind() == abstract_type::kind::map) {
        return make_shared<maps::putter>(receiver, std::move(v));
    } else {
        abort();
    }
}

bool
operation::addition::is_compatible_with(const std::unique_ptr<raw_update>& other) const {
    return !dynamic_cast<const set_value*>(other.get());
}

sstring
operation::subtraction::to_string(const column_definition& receiver) const {
    return format("{} = {} - {}", receiver.name_as_text(), receiver.name_as_text(), _value);
}

shared_ptr<operation>
operation::subtraction::prepare(data_dictionary::database db, const sstring& keyspace, const column_definition& receiver) const {
    auto ctype = dynamic_pointer_cast<const collection_type_impl>(receiver.type);
    if (!ctype) {
        if (!receiver.is_counter()) {
            throw exceptions::invalid_request_exception(format("Invalid operation ({}) for non counter column {}", to_string(receiver), receiver.name_as_text()));
        }
        auto v = prepare_expression(_value, db, keyspace, nullptr, receiver.column_specification);
        verify_no_aggregate_functions(v, "SET clause");
        return make_shared<constants::subtracter>(receiver, std::move(v));
    }
    if (!ctype->is_multi_cell()) {
        throw exceptions::invalid_request_exception(
                format("Invalid operation ({}) for frozen collection column {}", to_string(receiver), receiver.name_as_text()));
    }

    if (ctype->get_kind() == abstract_type::kind::list) {
        auto v = prepare_expression(_value, db, keyspace, nullptr, receiver.column_specification);
        verify_no_aggregate_functions(v, "SET clause");
        return make_shared<lists::discarder>(receiver, std::move(v));
    } else if (ctype->get_kind() == abstract_type::kind::set) {
        auto v = prepare_expression(_value, db, keyspace, nullptr, receiver.column_specification);
        verify_no_aggregate_functions(v, "SET clause");
        return make_shared<sets::discarder>(receiver, std::move(v));
    } else if (ctype->get_kind() == abstract_type::kind::map) {
        auto&& mtype = dynamic_pointer_cast<const map_type_impl>(ctype);
        // The value for a map subtraction is actually a set
        auto&& vr = make_lw_shared<column_specification>(
                receiver.column_specification->ks_name,
                receiver.column_specification->cf_name,
                receiver.column_specification->name,
                set_type_impl::get_instance(mtype->get_keys_type(), false));
        auto v = prepare_expression(_value, db, keyspace, nullptr, std::move(vr));
        verify_no_aggregate_functions(v, "SET clause");
        return ::make_shared<sets::discarder>(receiver, std::move(v));
    }
    abort();
}

bool
operation::subtraction::is_compatible_with(const std::unique_ptr<raw_update>& other) const {
    return !dynamic_cast<const set_value*>(other.get());
}

sstring
operation::prepend::to_string(const column_definition& receiver) const {
    return format("{} = {} + {}", receiver.name_as_text(), _value, receiver.name_as_text());
}

shared_ptr<operation>
operation::prepend::prepare(data_dictionary::database db, const sstring& keyspace, const column_definition& receiver) const {
    auto v = prepare_expression(_value, db, keyspace, nullptr, receiver.column_specification);
    verify_no_aggregate_functions(v, "SET clause");

    if (!dynamic_cast<const list_type_impl*>(receiver.type.get())) {
        throw exceptions::invalid_request_exception(format("Invalid operation ({}) for non list column {}", to_string(receiver), receiver.name_as_text()));
    } else if (!receiver.type->is_multi_cell()) {
        throw exceptions::invalid_request_exception(format("Invalid operation ({}) for frozen list column {}", to_string(receiver), receiver.name_as_text()));
    }

    return make_shared<lists::prepender>(receiver, std::move(v));
}

bool
operation::prepend::is_compatible_with(const std::unique_ptr<raw_update>& other) const {
    return !dynamic_cast<const set_value*>(other.get());
}


::shared_ptr <operation>
operation::set_value::prepare(data_dictionary::database db, const sstring& keyspace, const column_definition& receiver) const {
    auto v = prepare_expression(_value, db, keyspace, nullptr, receiver.column_specification);
    verify_no_aggregate_functions(v, "SET clause");

    if (receiver.type->is_counter()) {
        throw exceptions::invalid_request_exception(format("Cannot set the value of counter column {} (counters can only be incremented/decremented, not set)", receiver.name_as_text()));
    }

    if (receiver.type->is_collection()) {
        auto k = receiver.type->get_kind();
        if (k == abstract_type::kind::list) {
            return make_shared<lists::setter>(receiver, std::move(v));
        } else if (k == abstract_type::kind::set) {
            return make_shared<sets::setter>(receiver, std::move(v));
        } else if (k == abstract_type::kind::map) {
            return make_shared<maps::setter>(receiver, std::move(v));
        } else {
            abort();
        }
    }

    if (receiver.type->is_user_type()) {
        return make_shared<user_types::setter>(receiver, std::move(v));
    }

    return ::make_shared<constants::setter>(receiver, std::move(v));
}

::shared_ptr <operation>
operation::set_counter_value_from_tuple_list::prepare(data_dictionary::database db, const sstring& keyspace, const column_definition& receiver) const {
    static thread_local const data_type counter_tuple_type = tuple_type_impl::get_instance({int32_type, uuid_type, long_type, long_type});
    static thread_local const data_type counter_tuple_list_type = list_type_impl::get_instance(counter_tuple_type, true);

    if (!receiver.type->is_counter()) {
        throw exceptions::invalid_request_exception(format("Column {} is not a counter", receiver.name_as_text()));
    }

    // We need to fake a column of list<tuple<...>> to prepare the value expression
    auto & os = receiver.column_specification;
    auto spec = make_lw_shared<cql3::column_specification>(os->ks_name, os->cf_name, os->name, counter_tuple_list_type);
    auto v = prepare_expression(_value, db, keyspace, nullptr, spec);
    verify_no_aggregate_functions(v, "SET clause");

    // Will not be used elsewhere, so make it local.
    class counter_setter : public operation_no_unset_support {
    public:
        using operation_no_unset_support::operation_no_unset_support;

        bool is_raw_counter_shard_write() const override {
            return true;
        }
        void execute(mutation& m, const clustering_key_prefix& prefix, const update_parameters& params) override {
            cql3::raw_value list_value = expr::evaluate(*_e, params._options);
            if (list_value.is_null()) {
                throw std::invalid_argument("Invalid input data to counter set");
            }

            utils::chunked_vector<managed_bytes_opt> list_elements = expr::get_list_elements(list_value);

            counter_id last(utils::UUID(0, 0));
            counter_cell_builder ccb(list_elements.size());
            for (auto& bo : list_elements) {
                if (!bo) {
                    throw exceptions::invalid_request_exception("Invalid NULL value in list of counter shards");
                }
                // lexical etc cast fails should be enough type checking here.
                auto tuple = value_cast<tuple_type_impl::native_type>(counter_tuple_type->deserialize(managed_bytes_view(*bo)));
                auto shard = value_cast<int>(tuple[0]);
                auto id = counter_id(value_cast<utils::UUID>(tuple[1]));
                auto clock = value_cast<int64_t>(tuple[2]);
                auto value = value_cast<int64_t>(tuple[3]);

                if (id <= last) {
                    throw marshal_exception(
                                    format("invalid counter id order, {} <= {}",
                                                    id.uuid(),
                                                    last.uuid()));
                }
                last = id;
                // TODO: maybe allow more than global values to propagate,
                // though we don't (yet at least) in sstable::partition so...
                switch (shard) {
                case 'g':
                    ccb.add_shard(counter_shard(id, value, clock));
                    break;
                case 'l':
                    throw marshal_exception("encountered a local shard in a counter cell");
                case 'r':
                    throw marshal_exception("encountered remote shards in a counter cell");
                default:
                    throw marshal_exception(format("encountered unknown shard {:d} in a counter cell", shard));
                }
            }
            // Note. this is a counter value cell, not an update.
            // see counters.cc, we need to detect this.
            m.set_cell(prefix, column, ccb.build(params.timestamp()));
        }
    };

    return make_shared<counter_setter>(receiver, std::move(v));
};

bool
operation::set_value::is_compatible_with(const std::unique_ptr<raw_update>& other) const {
    // We don't allow setting multiple time the same column, because 1)
    // it's stupid and 2) the result would seem random to the user.
    return false;
}

const column_identifier::raw&
operation::element_deletion::affected_column() const {
    return *_id;
}

shared_ptr<operation>
operation::element_deletion::prepare(data_dictionary::database db, const sstring& keyspace, const column_definition& receiver) const {
    if (!receiver.type->is_collection()) {
        throw exceptions::invalid_request_exception(format("Invalid deletion operation for non collection column {}", receiver.name_as_text()));
    } else if (!receiver.type->is_multi_cell()) {
        throw exceptions::invalid_request_exception(format("Invalid deletion operation for frozen collection column {}", receiver.name_as_text()));
    }
    auto ctype = static_pointer_cast<const collection_type_impl>(receiver.type);
    if (ctype->get_kind() == abstract_type::kind::list) {
        auto&& idx = prepare_expression(_element, db, keyspace, nullptr, lists::index_spec_of(*receiver.column_specification));
        verify_no_aggregate_functions(idx, "SET clause");
        return make_shared<lists::discarder_by_index>(receiver, std::move(idx));
    } else if (ctype->get_kind() == abstract_type::kind::set) {
        auto&& elt = prepare_expression(_element, db, keyspace, nullptr, sets::value_spec_of(*receiver.column_specification));
        verify_no_aggregate_functions(elt, "SET clause");
        return make_shared<sets::element_discarder>(receiver, std::move(elt));
    } else if (ctype->get_kind() == abstract_type::kind::map) {
        auto&& key = prepare_expression(_element, db, keyspace, nullptr, maps::key_spec_of(*receiver.column_specification));
        verify_no_aggregate_functions(key, "SET clause");
        return make_shared<maps::discarder_by_key>(receiver, std::move(key));
    }
    abort();
}

expr::expression
operation::prepare_new_value_for_broadcast_tables() const {
    // FIXME: implement for every type of `operation`.
    throw service::broadcast_tables::unsupported_operation_error{};
}

}
