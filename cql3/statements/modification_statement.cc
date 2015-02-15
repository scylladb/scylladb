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

#include "cql3/statements/modification_statement.hh"
#include "cql3/restrictions/single_column_restriction.hh"
#include "cql3/single_column_relation.hh"
#include "validation.hh"
#include "core/shared_ptr.hh"

namespace cql3 {

namespace statements {

::shared_ptr<column_identifier> modification_statement::CAS_RESULT_COLUMN = ::make_shared<column_identifier>("[applied]", false);

std::ostream&
operator<<(std::ostream& out, modification_statement::statement_type t) {
    switch (t) {
        case modification_statement::statement_type::UPDATE:
            out << "UPDATE";
            break;
        case modification_statement::statement_type::INSERT:
            out << "INSERT";
            break;
        case modification_statement::statement_type::DELETE:
            out << "DELETE";
            break;
    }
    return out;
}

future<std::vector<mutation>>
modification_statement::get_mutations(const query_options& options, bool local, int64_t now) {
    auto keys = make_lw_shared(build_partition_keys(options));
    auto prefix = make_lw_shared(create_clustering_prefix(options));
    return make_update_parameters(keys, prefix, options, local, now).then(
            [this, keys = std::move(keys), prefix = std::move(prefix), now] (auto params_ptr) {
                std::vector<mutation> mutations;
                mutations.reserve(keys->size());
                for (auto key : *keys) {
                    validation::validate_cql_key(s, key);
                    mutations.emplace_back(std::move(key), s);
                    auto& m = mutations.back();
                    this->add_update_for_key(m, *prefix, *params_ptr);
                }
                return make_ready_future<decltype(mutations)>(std::move(mutations));
            });
}

future<std::unique_ptr<update_parameters>>
modification_statement::make_update_parameters(
        lw_shared_ptr<std::vector<partition_key>> keys,
        lw_shared_ptr<clustering_prefix> prefix,
        const query_options& options,
        bool local,
        int64_t now) {
    return read_required_rows(std::move(keys), std::move(prefix), local, options.get_consistency()).then(
            [this, &options, now] (auto rows) {
                return make_ready_future<std::unique_ptr<update_parameters>>(
                        std::make_unique<update_parameters>(s, options,
                                this->get_timestamp(now, options),
                                this->get_time_to_live(options),
                                std::move(rows)));
            });
}

future<update_parameters::prefetched_rows_type>
modification_statement::read_required_rows(
        lw_shared_ptr<std::vector<partition_key>> keys,
        lw_shared_ptr<clustering_prefix> prefix,
        bool local,
        db::consistency_level cl) {
    if (!requires_read()) {
        return make_ready_future<update_parameters::prefetched_rows_type>(
                update_parameters::prefetched_rows_type{});
    }
    throw std::runtime_error("NOT IMPLEMENTED");
#if 0
        try
        {
            cl.validateForRead(keyspace());
        }
        catch (InvalidRequestException e)
        {
            throw new InvalidRequestException(String.format("Write operation require a read but consistency %s is not supported on reads", cl));
        }

        ColumnSlice[] slices = new ColumnSlice[]{ clusteringPrefix.slice() };
        List<ReadCommand> commands = new ArrayList<ReadCommand>(partitionKeys.size());
        long now = System.currentTimeMillis();
        for (ByteBuffer key : partitionKeys)
            commands.add(new SliceFromReadCommand(keyspace(),
                                                  key,
                                                  columnFamily(),
                                                  now,
                                                  new SliceQueryFilter(slices, false, Integer.MAX_VALUE)));

        List<Row> rows = local
                       ? SelectStatement.readLocally(keyspace(), commands)
                       : StorageProxy.read(commands, cl);

        Map<ByteBuffer, CQL3Row> map = new HashMap<ByteBuffer, CQL3Row>();
        for (Row row : rows)
        {
            if (row.cf == null || row.cf.isEmpty())
                continue;

            Iterator<CQL3Row> iter = cfm.comparator.CQL3RowBuilder(cfm, now).group(row.cf.getSortedColumns().iterator());
            if (iter.hasNext())
            {
                map.put(row.key.getKey(), iter.next());
                // We can only update one CQ3Row per partition key at a time (we don't allow IN for clustering key)
                assert !iter.hasNext();
            }
        }
        return map;
#endif
}

const column_definition*
modification_statement::get_first_empty_key() {
    for (auto& def : s->clustering_key) {
        if (_processed_keys.find(&def) == _processed_keys.end()) {
            return &def;
        }
    }
    return {};
}

clustering_prefix
modification_statement::create_clustering_prefix_internal(const query_options& options) {
    std::vector<bytes_opt> components;
    const column_definition* first_empty_key = nullptr;

    for (auto& def : s->clustering_key) {
        auto i = _processed_keys.find(&def);
        if (i == _processed_keys.end()) {
            first_empty_key = &def;
            // Tomek: Origin had "&& s->comparator->is_composite()" in the condition below.
            // Comparator is a thrift concept, not CQL concept, and we want to avoid
            // using thrift concepts here. I think it's safe to drop this here because the only
            // case in which we would get a non-composite comparator here would be if the cell
            // name type is SimpleSparse, which means:
            //   (a) CQL compact table without clustering columns
            //   (b) thrift static CF with non-composite comparator
            // Those tables don't have clustering columns so we wouldn't reach this code, thus
            // the check seems redundant.
            if (require_full_clustering_key() && !s->is_dense()) {
                throw exceptions::invalid_request_exception(sprint("Missing mandatory PRIMARY KEY part %s", def.name_as_text()));
            }
        } else if (first_empty_key) {
            throw exceptions::invalid_request_exception(sprint("Missing PRIMARY KEY part %s since %s is set", first_empty_key->name_as_text(), def.name_as_text()));
        } else {
            auto values = i->second->values(options);
            assert(values.size() == 1);
            auto val = values[0];
            if (!val) {
                throw exceptions::invalid_request_exception(sprint("Invalid null value for clustering key part %s", def.name_as_text()));
            }
            components.push_back(val);
        }
    }
    return components;
}

clustering_prefix
modification_statement::create_clustering_prefix(const query_options& options) {
    // If the only updated/deleted columns are static, then we don't need clustering columns.
    // And in fact, unless it is an INSERT, we reject if clustering columns are provided as that
    // suggest something unintended. For instance, given:
    //   CREATE TABLE t (k int, v int, s int static, PRIMARY KEY (k, v))
    // it can make sense to do:
    //   INSERT INTO t(k, v, s) VALUES (0, 1, 2)
    // but both
    //   UPDATE t SET s = 3 WHERE k = 0 AND v = 1
    //   DELETE v FROM t WHERE k = 0 AND v = 1
    // sounds like you don't really understand what your are doing.
    if (_sets_static_columns && !_sets_regular_columns) {
        // If we set no non-static columns, then it's fine not to have clustering columns
        if (_has_no_clustering_columns) {
            return {};
        }

        // If we do have clustering columns however, then either it's an INSERT and the query is valid
        // but we still need to build a proper prefix, or it's not an INSERT, and then we want to reject
        // (see above)
        if (type != statement_type::INSERT) {
            for (auto& def : s->clustering_key) {
                if (_processed_keys.count(&def)) {
                    throw exceptions::invalid_request_exception(sprint(
                            "Invalid restriction on clustering column %s since the %s statement modifies only static columns",
                            def.name_as_text(), type));
                }
            }

            // we should get there as it contradicts _has_no_clustering_columns == false
            throw std::logic_error("contradicts _has_no_clustering_columns == false");
        }
    }

    return create_clustering_prefix_internal(options);
}

std::vector<partition_key>
modification_statement::build_partition_keys(const query_options& options) {
    std::vector<partition_key> result;
    std::vector<bytes_opt> components;

    auto remaining = s->partition_key.size();

    for (auto& def : s->partition_key) {
        auto i = _processed_keys.find(&def);
        if (i == _processed_keys.end()) {
            throw exceptions::invalid_request_exception(sprint("Missing mandatory PRIMARY KEY part %s", def.name_as_text()));
        }

        auto values = i->second->values(options);

        if (remaining == 1) {
            if (values.size() == 1) {
                auto val = values[0];
                if (!val) {
                    throw exceptions::invalid_request_exception(sprint("Invalid null value for partition key part %s", def.name_as_text()));
                }
                components.push_back(val);
                partition_key key = serialize_value(*s->partition_key_type, components);
                validation::validate_cql_key(s, key);
                result.push_back(key);
            } else {
                for (auto&& val : values) {
                    if (!val) {
                        throw exceptions::invalid_request_exception(sprint("Invalid null value for partition key part %s", def.name_as_text()));
                    }
                    std::vector<bytes_opt> full_components;
                    full_components.reserve(components.size() + 1);
                    auto i = std::copy(components.begin(), components.end(), std::back_inserter(full_components));
                    *i = val;
                    partition_key key = serialize_value(*s->partition_key_type, full_components);
                    validation::validate_cql_key(s, key);
                    result.push_back(key);
                }
            }
        } else {
            if (values.size() != 1) {
                throw exceptions::invalid_request_exception("IN is only supported on the last column of the partition key");
            }
            auto val = values[0];
            if (!val) {
                throw exceptions::invalid_request_exception(sprint("Invalid null value for partition key part %s", def.name_as_text()));
            }
            components.push_back(val);
        }

        remaining--;
    }
    return result;
}

future<::shared_ptr<transport::messages::result_message>>
modification_statement::execute(service::storage_proxy& proxy, service::query_state& qs, const query_options& options) {
    if (has_conditions() && options.get_protocol_version() == 1) {
        throw new exceptions::invalid_request_exception("Conditional updates are not supported by the protocol version in use. You need to upgrade to a driver using the native protocol v2.");
    }

    if (has_conditions()) {
        return execute_with_condition(proxy, qs, options);
    }

    return execute_without_condition(proxy, qs, options).then([] {
        return make_ready_future<::shared_ptr<transport::messages::result_message>>(
                ::shared_ptr<transport::messages::result_message>{});
    });
}

future<>
modification_statement::execute_without_condition(service::storage_proxy& proxy, service::query_state& qs, const query_options& options) {
    auto cl = options.get_consistency();
    if (is_counter()) {
        db::validate_counter_for_write(s, cl);
    } else {
        db::validate_for_write(s->ks_name, cl);
    }

    return get_mutations(options, false, options.get_timestamp(qs)).then([cl, &proxy] (auto mutations) {
        if (mutations.empty()) {
            return now();
        }
        return proxy.mutate_with_triggers(std::move(mutations), cl, false);
    });
}

future<::shared_ptr<transport::messages::result_message>>
modification_statement::execute_with_condition(service::storage_proxy& proxy, service::query_state& qs, const query_options& options) {
    unimplemented::lwt();
#if 0
        List<ByteBuffer> keys = buildPartitionKeyNames(options);
        // We don't support IN for CAS operation so far
        if (keys.size() > 1)
            throw new InvalidRequestException("IN on the partition key is not supported with conditional updates");

        ByteBuffer key = keys.get(0);
        long now = options.getTimestamp(queryState);
        Composite prefix = createClusteringPrefix(options);

        CQL3CasRequest request = new CQL3CasRequest(cfm, key, false);
        addConditions(prefix, request, options);
        request.addRowUpdate(prefix, this, options, now);

        ColumnFamily result = StorageProxy.cas(keyspace(),
                                               columnFamily(),
                                               key,
                                               request,
                                               options.getSerialConsistency(),
                                               options.getConsistency(),
                                               queryState.getClientState());
        return new ResultMessage.Rows(buildCasResultSet(key, result, options));
#endif
}

void
modification_statement::add_key_values(column_definition& def, ::shared_ptr<restrictions::restriction> values) {
    if (def.kind == column_definition::CLUSTERING) {
        _has_no_clustering_columns = false;
    }

    auto insert_result = _processed_keys.insert({&def, values});
    if (!insert_result.second) {
        throw exceptions::invalid_request_exception(sprint("Multiple definitions found for PRIMARY KEY part %s", def.name_as_text()));
    }
}

void
modification_statement::add_key_value(column_definition& def, ::shared_ptr<term> value) {
    add_key_values(def, ::make_shared<restrictions::single_column_restriction::EQ>(def, value));
}

void
modification_statement::process_where_clause(std::vector<relation_ptr> where_clause, ::shared_ptr<variable_specifications> names) {
    for (auto&& relation : where_clause) {
        if (relation->is_multi_column()) {
            throw exceptions::invalid_request_exception(sprint("Multi-column relations cannot be used in WHERE clauses for UPDATE and DELETE statements: %s", relation->to_string()));
        }

        auto rel = dynamic_pointer_cast<single_column_relation>(relation);
        if (rel->on_token()) {
            throw exceptions::invalid_request_exception(sprint("The token function cannot be used in WHERE clauses for UPDATE and DELETE statements: %s", relation->to_string()));
        }

        auto id = rel->get_entity()->prepare_column_identifier(s);
        auto def = get_column_definition(s, *id);
        if (!def) {
            throw exceptions::invalid_request_exception(sprint("Unknown key identifier %s", *id));
        }

        switch (def->kind) {
            case column_definition::column_kind::PARTITION:
            case column_definition::column_kind::CLUSTERING:
                if (rel->is_EQ() || (def->is_partition_key() && rel->is_IN())) {
                    add_key_values(*def, rel->to_restriction(s, std::move(names)));
                    break;
                }
                throw exceptions::invalid_request_exception(sprint("Invalid operator %s for PRIMARY KEY part %s", rel->get_operator(), def->name_as_text()));
            default:
                throw exceptions::invalid_request_exception(sprint("Non PRIMARY KEY %s found in where clause", def->name_as_text()));
        }
    }
}

std::unique_ptr<parsed_statement::prepared>
modification_statement::parsed::prepare(database& db) {
    auto bound_names = get_bound_variables();
    auto statement = prepare(db, bound_names);
    return std::make_unique<parsed_statement::prepared>(std::move(statement), *bound_names);
}

::shared_ptr<modification_statement>
modification_statement::parsed::prepare(database& db, ::shared_ptr<variable_specifications> bound_names) {
    schema_ptr schema = validation::validate_column_family(db, keyspace(), column_family());

    auto prepared_attributes = _attrs->prepare(keyspace(), column_family());
    prepared_attributes->collect_marker_specification(bound_names);

    ::shared_ptr<modification_statement> stmt = prepare_internal(schema, bound_names, std::move(prepared_attributes));

    if (_if_not_exists || _if_exists || !_conditions.empty()) {
        if (stmt->is_counter()) {
            throw exceptions::invalid_request_exception("Conditional updates are not supported on counter tables");
        }
        if (_attrs->timestamp) {
            throw exceptions::invalid_request_exception("Cannot provide custom timestamp for conditional updates");
        }

        if (_if_not_exists) {
            // To have both 'IF NOT EXISTS' and some other conditions doesn't make sense.
            // So far this is enforced by the parser, but let's assert it for sanity if ever the parse changes.
            assert(_conditions.empty());
            assert(!_if_exists);
            stmt->set_if_not_exist_condition();
        } else if (_if_exists) {
            assert(_conditions.empty());
            assert(!_if_not_exists);
            stmt->set_if_exist_condition();
        } else {
            for (auto&& entry : _conditions) {
                auto id = entry.first->prepare_column_identifier(schema);
                column_definition* def = get_column_definition(schema, *id);
                if (!def) {
                    throw exceptions::invalid_request_exception(sprint("Unknown identifier %s", *id));
                }

                auto condition = entry.second->prepare(keyspace(), *def);
                condition->collect_marker_specificaton(bound_names);

                switch (def->kind) {
                    case column_definition::PARTITION:
                    case column_definition::CLUSTERING:
                        throw exceptions::invalid_request_exception(sprint("PRIMARY KEY column '%s' cannot have IF conditions", *id));
                    default:
                        stmt->add_condition(condition);
                }
            }
        }
        stmt->validate_where_clause_for_conditions();
    }
    return stmt;
}

void
modification_statement::validate(const service::client_state& state) {
    if (has_conditions() && attrs->is_timestamp_set()) {
        throw exceptions::invalid_request_exception("Cannot provide custom timestamp for conditional updates");
    }

    if (is_counter() && attrs->is_timestamp_set()) {
        throw exceptions::invalid_request_exception("Cannot provide custom timestamp for counter updates");
    }

    if (is_counter() && attrs->is_time_to_live_set()) {
        throw exceptions::invalid_request_exception("Cannot provide custom TTL for counter updates");
    }
}

}

}
