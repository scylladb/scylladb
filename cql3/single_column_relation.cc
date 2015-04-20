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
 * Copyright 2015 Cloudius Systems
 *
 * Modified by Cloudius Systems
 */

#include "cql3/single_column_relation.hh"
#include "cql3/restrictions/single_column_restriction.hh"
#include "unimplemented.hh"

using namespace cql3::restrictions;

namespace cql3 {

::shared_ptr<term>
single_column_relation::to_term(const std::vector<::shared_ptr<column_specification>>& receivers,
                                ::shared_ptr<term::raw> raw,
                                database& db,
                                const sstring& keyspace,
                                ::shared_ptr<variable_specifications> bound_names) {
    // TODO: optimize vector away, accept single column_specification
    assert(receivers.size() == 1);
    auto term = raw->prepare(db, keyspace, receivers[0]);
    term->collect_marker_specification(bound_names);
    return term;
}

::shared_ptr<restrictions::restriction>
single_column_relation::new_EQ_restriction(database& db, schema_ptr schema, ::shared_ptr<variable_specifications> bound_names) {
    const column_definition& column_def = to_column_definition(schema, _entity);
    if (!_map_key) {
        auto term = to_term(to_receivers(schema, column_def), _value, db, schema->ks_name, bound_names);
        return ::make_shared<single_column_restriction::EQ>(column_def, std::move(term));
    }
    fail(unimplemented::cause::COLLECTIONS);
#if 0
        List<? extends ColumnSpecification> receivers = toReceivers(schema, columnDef);
        Term entryKey = toTerm(Collections.singletonList(receivers.get(0)), map_key, schema.ksName, bound_names);
        Term entryValue = toTerm(Collections.singletonList(receivers.get(1)), value, schema.ksName, bound_names);
        return new SingleColumnRestriction.Contains(columnDef, entryKey, entryValue);
#endif
}

::shared_ptr<restrictions::restriction>
single_column_relation::new_IN_restriction(database& db, schema_ptr schema, ::shared_ptr<variable_specifications> bound_names) {
    const column_definition& column_def = to_column_definition(schema, _entity);
    auto receivers = to_receivers(schema, column_def);
    auto terms = to_terms(receivers, _in_values, db, schema->ks_name, bound_names);
    if (terms.empty()) {
        fail(unimplemented::cause::COLLECTIONS);
#if 0
        auto term = to_term(receivers, _value, schema->ks_name, bound_names);
        return new SingleColumnRestriction.InWithMarker(columnDef, (Lists.Marker) term);
#endif
    }
    return ::make_shared<single_column_restriction::IN_with_values>(column_def, std::move(terms));
}

std::vector<::shared_ptr<column_specification>>
single_column_relation::to_receivers(schema_ptr schema, const column_definition& column_def)
{
    auto receiver = column_def.column_specification;

    if (column_def.is_compact_value()) {
        throw exceptions::invalid_request_exception(sprint(
            "Predicates on the non-primary-key column (%s) of a COMPACT table are not yet supported", column_def.name_as_text()));
    }

    if (is_IN()) {
        // For partition keys we only support IN for the last name so far
        if (column_def.is_partition_key() && !schema->is_last_partition_key(column_def)) {
            throw exceptions::invalid_request_exception(sprint(
                "Partition KEY part %s cannot be restricted by IN relation (only the last part of the partition key can)",
                column_def.name_as_text()));
        }

        // We only allow IN on the row key and the clustering key so far, never on non-PK columns, and this even if
        // there's an index
        // Note: for backward compatibility reason, we conside a IN of 1 value the same as a EQ, so we let that
        // slide.
        if (!column_def.is_primary_key() && !can_have_only_one_value()) {
            throw exceptions::invalid_request_exception(sprint(
                   "IN predicates on non-primary-key columns (%s) is not yet supported", column_def.name_as_text()));
        }
    } else if (is_slice()) {
        // Non EQ relation is not supported without token(), even if we have a 2ndary index (since even those
        // are ordered by partitioner).
        // Note: In theory we could allow it for 2ndary index queries with ALLOW FILTERING, but that would
        // probably require some special casing
        // Note bis: This is also why we don't bother handling the 'tuple' notation of #4851 for keys. If we
        // lift the limitation for 2ndary
        // index with filtering, we'll need to handle it though.
        if (column_def.is_partition_key()) {
            throw exceptions::invalid_request_exception(
                "Only EQ and IN relation are supported on the partition key (unless you use the token() function)");
        }
    }

    if (is_contains_key()) {
        fail(unimplemented::cause::COLLECTIONS);
#if 0
        if (!(receiver.type instanceof MapType)) {
            throw exceptions::invalid_request_exception(sprint("Cannot use CONTAINS KEY on non-map column %s", receiver.name_as_text()));
        }
#endif
    }

    if (_map_key) {
        fail(unimplemented::cause::COLLECTIONS);
#if 0
        checkFalse(receiver.type instanceof ListType, "Indexes on list entries (%s[index] = value) are not currently supported.", receiver.name);
        checkTrue(receiver.type instanceof MapType, "Column %s cannot be used as a map", receiver.name);
        checkTrue(receiver.type.isMultiCell(), "Map-entry equality predicates on frozen map column %s are not supported", receiver.name);
        checkTrue(isEQ(), "Only EQ relations are supported on map entries");
#endif
    }

    if (receiver->type->is_collection()) {
        fail(unimplemented::cause::COLLECTIONS);
#if 0
        // We don't support relations against entire collections (unless they're frozen), like "numbers = {1, 2, 3}"
        checkFalse(receiver.type.isMultiCell() && !isLegalRelationForNonFrozenCollection(),
                   "Collection column '%s' (%s) cannot be restricted by a '%s' relation",
                   receiver.name,
                   receiver.type.asCQL3Type(),
                   get_operator());

        if (isContainsKey() || isContains())
        {
            receiver = makeCollectionReceiver(receiver, isContainsKey());
        }
        else if (receiver.type.isMultiCell() && map_key != null && isEQ())
        {
            List<ColumnSpecification> receivers = new ArrayList<>(2);
            receivers.add(makeCollectionReceiver(receiver, true));
            receivers.add(makeCollectionReceiver(receiver, false));
            return receivers;
        }
#endif
    }

    return {std::move(receiver)};
}

}
