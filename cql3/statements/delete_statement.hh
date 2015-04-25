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

#pragma once

#include "cql3/statements/modification_statement.hh"
#include "cql3/attributes.hh"
#include "cql3/operation.hh"
#include "database_fwd.hh"

namespace cql3 {

namespace statements {

#if 0
import java.nio.ByteBuffer;
import java.util.*;

import com.google.common.collect.Iterators;

import org.apache.cassandra.cql3.*;
import org.apache.cassandra.cql3.restrictions.Restriction;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.composites.Composite;
import org.apache.cassandra.exceptions.*;
import org.apache.cassandra.utils.Pair;
#endif

/**
* A <code>DELETE</code> parsed from a CQL query statement.
*/
class delete_statement : public modification_statement {
public:
    delete_statement(statement_type type, uint32_t bound_terms, schema_ptr s, std::unique_ptr<attributes> attrs)
            : modification_statement{type, bound_terms, std::move(s), std::move(attrs)}
    { }

    virtual bool require_full_clustering_key() const override {
        return false;
    }

    virtual void add_update_for_key(mutation& m, const exploded_clustering_prefix& prefix, const update_parameters& params) override;

#if 0
    protected void validateWhereClauseForConditions() throws InvalidRequestException
    {
        Iterator<ColumnDefinition> iterator = Iterators.concat(cfm.partitionKeyColumns().iterator(), cfm.clusteringColumns().iterator());
        while (iterator.hasNext())
        {
            ColumnDefinition def = iterator.next();
            Restriction restriction = processedKeys.get(def.name);
            if (restriction == null || !(restriction.isEQ() || restriction.isIN()))
            {
                throw new InvalidRequestException(
                        String.format("DELETE statements must restrict all PRIMARY KEY columns with equality relations in order " +
                                      "to use IF conditions, but column '%s' is not restricted", def.name));
            }
        }

    }
#endif

    class parsed : public modification_statement::parsed {
    private:
        std::vector<::shared_ptr<operation::raw_deletion>> _deletions;
        std::vector<::shared_ptr<relation>> _where_clause;
    public:
        parsed(::shared_ptr<cf_name> name,
               ::shared_ptr<attributes::raw> attrs,
               std::vector<::shared_ptr<operation::raw_deletion>> deletions,
               std::vector<::shared_ptr<relation>> where_clause,
               conditions_vector conditions,
               bool if_exists)
            : modification_statement::parsed(std::move(name), std::move(attrs), std::move(conditions), false, if_exists)
            , _deletions(std::move(deletions))
            , _where_clause(std::move(where_clause))
        { }
    protected:
        virtual ::shared_ptr<modification_statement> prepare_internal(database& db, schema_ptr schema,
            ::shared_ptr<variable_specifications> bound_names, std::unique_ptr<attributes> attrs);
    };
};

}

}
