/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Copyright (C) 2015 ScyllaDB
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
#include <utility>

#include "operation.hh"
#include "operation_impl.hh"
#include "maps.hh"
#include "sets.hh"
#include "lists.hh"

namespace cql3 {

sstring
operation::set_element::to_string(const column_definition& receiver) const {
    return format("{}[{}] = {}", receiver.name_as_text(), *_selector, *_value);
}

shared_ptr<operation>
operation::set_element::prepare(database& db, const sstring& keyspace, const column_definition& receiver) {
    using exceptions::invalid_request_exception;
    auto rtype = dynamic_pointer_cast<const collection_type_impl>(receiver.type);
    if (!rtype) {
        throw invalid_request_exception(sprint("Invalid operation (%s) for non collection column %s", to_string(receiver), receiver.name()));
    } else if (!rtype->is_multi_cell()) {
        throw invalid_request_exception(sprint("Invalid operation (%s) for frozen collection column %s", to_string(receiver), receiver.name()));
    }

    if (&rtype->_kind == &collection_type_impl::kind::list) {
        auto&& lval = _value->prepare(db, keyspace, lists::value_spec_of(receiver.column_specification));
        if (_by_uuid) {
            auto&& idx = _selector->prepare(db, keyspace, lists::uuid_index_spec_of(receiver.column_specification));
            return make_shared<lists::setter_by_uuid>(receiver, idx, lval);
        } else {
            auto&& idx = _selector->prepare(db, keyspace, lists::index_spec_of(receiver.column_specification));
            return make_shared<lists::setter_by_index>(receiver, idx, lval);
        }
    } else if (&rtype->_kind == &collection_type_impl::kind::set) {
        throw invalid_request_exception(sprint("Invalid operation (%s) for set column %s", to_string(receiver), receiver.name()));
    } else if (&rtype->_kind == &collection_type_impl::kind::map) {
        auto key = _selector->prepare(db, keyspace, maps::key_spec_of(*receiver.column_specification));
        auto mval = _value->prepare(db, keyspace, maps::value_spec_of(*receiver.column_specification));
        return make_shared<maps::setter_by_key>(receiver, key, mval);
    }
    abort();
}

bool
operation::set_element::is_compatible_with(shared_ptr<raw_update> other) {
    // TODO: we could check that the other operation is not setting the same element
    // too (but since the index/key set may be a bind variables we can't always do it at this point)
    return !dynamic_pointer_cast<set_value>(std::move(other));
}

sstring
operation::addition::to_string(const column_definition& receiver) const {
    return format("{} = {} + {}", receiver.name_as_text(), receiver.name_as_text(), *_value);
}

shared_ptr<operation>
operation::addition::prepare(database& db, const sstring& keyspace, const column_definition& receiver) {
    auto v = _value->prepare(db, keyspace, receiver.column_specification);

    auto ctype = dynamic_pointer_cast<const collection_type_impl>(receiver.type);
    if (!ctype) {
        if (!receiver.is_counter()) {
            throw exceptions::invalid_request_exception(sprint("Invalid operation (%s) for non counter column %s", to_string(receiver), receiver.name()));
        }
        return make_shared<constants::adder>(receiver, v);
    } else if (!ctype->is_multi_cell()) {
        throw exceptions::invalid_request_exception(sprint("Invalid operation (%s) for frozen collection column %s", to_string(receiver), receiver.name()));
    }

    if (&ctype->_kind == &collection_type_impl::kind::list) {
        return make_shared<lists::appender>(receiver, v);
    } else if (&ctype->_kind == &collection_type_impl::kind::set) {
        return make_shared<sets::adder>(receiver, v);
    } else if (&ctype->_kind == &collection_type_impl::kind::map) {
        return make_shared<maps::putter>(receiver, v);
    } else {
        abort();
    }
}

bool
operation::addition::is_compatible_with(shared_ptr<raw_update> other) {
    return !dynamic_pointer_cast<set_value>(other);
}

sstring
operation::subtraction::to_string(const column_definition& receiver) const {
    return format("{} = {} - {}", receiver.name_as_text(), receiver.name_as_text(), *_value);
}

shared_ptr<operation>
operation::subtraction::prepare(database& db, const sstring& keyspace, const column_definition& receiver) {
    auto ctype = dynamic_pointer_cast<const collection_type_impl>(receiver.type);
    if (!ctype) {
        if (!receiver.is_counter()) {
            throw exceptions::invalid_request_exception(sprint("Invalid operation (%s) for non counter column %s", to_string(receiver), receiver.name()));
        }
        auto v = _value->prepare(db, keyspace, receiver.column_specification);
        return make_shared<constants::subtracter>(receiver, v);
    }
    if (!ctype->is_multi_cell()) {
        throw exceptions::invalid_request_exception(
                sprint("Invalid operation (%s) for frozen collection column %s", to_string(receiver), receiver.name()));
    }

    if (&ctype->_kind == &collection_type_impl::kind::list) {
        return make_shared<lists::discarder>(receiver, _value->prepare(db, keyspace, receiver.column_specification));
    } else if (&ctype->_kind == &collection_type_impl::kind::set) {
        return make_shared<sets::discarder>(receiver, _value->prepare(db, keyspace, receiver.column_specification));
    } else if (&ctype->_kind == &collection_type_impl::kind::map) {
        auto&& mtype = dynamic_pointer_cast<const map_type_impl>(ctype);
        // The value for a map subtraction is actually a set
        auto&& vr = make_shared<column_specification>(
                receiver.column_specification->ks_name,
                receiver.column_specification->cf_name,
                receiver.column_specification->name,
                set_type_impl::get_instance(mtype->get_keys_type(), false));
        return make_shared<sets::discarder>(receiver, _value->prepare(db, keyspace, std::move(vr)));
    }
    abort();
}

bool
operation::subtraction::is_compatible_with(shared_ptr<raw_update> other) {
    return !dynamic_pointer_cast<set_value>(other);
}

sstring
operation::prepend::to_string(const column_definition& receiver) const {
    return format("{} = {} + {}", receiver.name_as_text(), *_value, receiver.name_as_text());
}

shared_ptr<operation>
operation::prepend::prepare(database& db, const sstring& keyspace, const column_definition& receiver) {
    auto v = _value->prepare(db, keyspace, receiver.column_specification);

    if (!dynamic_cast<const list_type_impl*>(receiver.type.get())) {
        throw exceptions::invalid_request_exception(sprint("Invalid operation (%s) for non list column %s", to_string(receiver), receiver.name()));
    } else if (!receiver.type->is_multi_cell()) {
        throw exceptions::invalid_request_exception(sprint("Invalid operation (%s) for frozen list column %s", to_string(receiver), receiver.name()));
    }

    return make_shared<lists::prepender>(receiver, std::move(v));
}

bool
operation::prepend::is_compatible_with(shared_ptr<raw_update> other) {
    return !dynamic_pointer_cast<set_value>(other);
}


::shared_ptr <operation>
operation::set_value::prepare(database& db, const sstring& keyspace, const column_definition& receiver) {
    auto v = _value->prepare(db, keyspace, receiver.column_specification);

    if (receiver.type->is_counter()) {
        throw exceptions::invalid_request_exception(sprint("Cannot set the value of counter column %s (counters can only be incremented/decremented, not set)", receiver.name_as_text()));
    }

    if (!receiver.type->is_collection()) {
        return ::make_shared<constants::setter>(receiver, v);
    }

    auto& k = static_pointer_cast<const collection_type_impl>(receiver.type)->_kind;
    if (&k == &collection_type_impl::kind::list) {
        return make_shared<lists::setter>(receiver, v);
    } else if (&k == &collection_type_impl::kind::set) {
        return make_shared<sets::setter>(receiver, v);
    } else if (&k == &collection_type_impl::kind::map) {
        return make_shared<maps::setter>(receiver, v);
    } else {
        abort();
    }
}

::shared_ptr <operation>
operation::set_counter_value_from_tuple_list::prepare(database& db, const sstring& keyspace, const column_definition& receiver) {
    static thread_local const data_type counter_tuple_type = tuple_type_impl::get_instance({int32_type, uuid_type, long_type, long_type});
    static thread_local const data_type counter_tuple_list_type = list_type_impl::get_instance(counter_tuple_type, true);

    if (!receiver.type->is_counter()) {
        throw exceptions::invalid_request_exception(sprint("Column %s is not a counter", receiver.name_as_text()));
    }

    // We need to fake a column of list<tuple<...>> to prepare the value term
    auto & os = receiver.column_specification;
    auto spec = make_shared<cql3::column_specification>(os->ks_name, os->cf_name, os->name, counter_tuple_list_type);
    auto v = _value->prepare(db, keyspace, spec);

    // Will not be used elsewhere, so make it local.
    class counter_setter : public operation {
    public:
        using operation::operation;

        bool is_raw_counter_shard_write() const override {
            return true;
        }
        void execute(mutation& m, const clustering_key_prefix& prefix, const update_parameters& params) override {
            const auto& value = _t->bind(params._options);
            auto&& list_value = dynamic_pointer_cast<lists::value>(value);

            if (!list_value) {
                throw std::invalid_argument("Invalid input data to counter set");
            }

            counter_id last(utils::UUID(0, 0));
            counter_cell_builder ccb(list_value->_elements.size());
            for (auto& bo : list_value->_elements) {
                // lexical etc cast fails should be enough type checking here.
                auto tuple = value_cast<tuple_type_impl::native_type>(counter_tuple_type->deserialize(*bo));
                auto shard = value_cast<int>(tuple[0]);
                auto id = counter_id(value_cast<utils::UUID>(tuple[1]));
                auto clock = value_cast<int64_t>(tuple[2]);
                auto value = value_cast<int64_t>(tuple[3]);

                using namespace std::rel_ops;

                if (id <= last) {
                    throw marshal_exception(
                                    sprint("invalid counter id order, %s <= %s",
                                                    id.to_uuid().to_sstring(),
                                                    last.to_uuid().to_sstring()));
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
                    throw marshal_exception(sprint("encountered unknown shard %d in a counter cell", shard));
                }
            }
            // Note. this is a counter value cell, not an update.
            // see counters.cc, we need to detect this.
            m.set_cell(prefix, column, ccb.build(params.timestamp()));
        }
    };

    return make_shared<counter_setter>(receiver, v);
};

bool
operation::set_value::is_compatible_with(::shared_ptr <raw_update> other) {
    // We don't allow setting multiple time the same column, because 1)
    // it's stupid and 2) the result would seem random to the user.
    return false;
}

shared_ptr<column_identifier::raw>
operation::element_deletion::affected_column() {
    return _id;
}

shared_ptr<operation>
operation::element_deletion::prepare(database& db, const sstring& keyspace, const column_definition& receiver) {
    if (!receiver.type->is_collection()) {
        throw exceptions::invalid_request_exception(sprint("Invalid deletion operation for non collection column %s", receiver.name()));
    } else if (!receiver.type->is_multi_cell()) {
        throw exceptions::invalid_request_exception(sprint("Invalid deletion operation for frozen collection column %s", receiver.name()));
    }
    auto ctype = static_pointer_cast<const collection_type_impl>(receiver.type);
    if (&ctype->_kind == &collection_type_impl::kind::list) {
        auto&& idx = _element->prepare(db, keyspace, lists::index_spec_of(receiver.column_specification));
        return make_shared<lists::discarder_by_index>(receiver, std::move(idx));
    } else if (&ctype->_kind == &collection_type_impl::kind::set) {
        auto&& elt = _element->prepare(db, keyspace, sets::value_spec_of(receiver.column_specification));
        return make_shared<sets::element_discarder>(receiver, std::move(elt));
    } else if (&ctype->_kind == &collection_type_impl::kind::map) {
        auto&& key = _element->prepare(db, keyspace, maps::key_spec_of(*receiver.column_specification));
        return make_shared<maps::discarder_by_key>(receiver, std::move(key));
    }
    abort();
}

}
