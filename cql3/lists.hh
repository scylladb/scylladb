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

#ifndef CQL3_LISTS_HH
#define CQL3_LISTS_HH

#include "cql3/abstract_marker.hh"
#include "to_string.hh"
#include "utils/UUID_gen.hh"
#include "operation.hh"

namespace cql3 {

#if 0
package org.apache.cassandra.cql3;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.Cell;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.composites.CellName;
import org.apache.cassandra.db.composites.Composite;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.serializers.CollectionSerializer;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.transport.Server;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.UUIDGen;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
#endif

/**
 * Static helper methods and classes for lists.
 */
class lists {
    lists() = delete;
public:
    static shared_ptr<column_specification> index_spec_of(shared_ptr<column_specification> column);
    static shared_ptr<column_specification> value_spec_of(shared_ptr<column_specification> column);

    class literal : public term::raw {
        const std::vector<shared_ptr<term::raw>> _elements;
    public:
        explicit literal(std::vector<shared_ptr<term::raw>> elements)
            : _elements(std::move(elements)) {
        }
        shared_ptr<term> prepare(const sstring& keyspace, shared_ptr<column_specification> receiver);
    private:
        void validate_assignable_to(const sstring keyspace, shared_ptr<column_specification> receiver);
    public:
        virtual assignment_testable::test_result test_assignment(const sstring& keyspace, shared_ptr<column_specification> receiver) override;
        virtual sstring to_string() const override;
    };

    class value : public multi_item_terminal, collection_terminal {
        std::vector<bytes_opt> _elements;
    public:
        explicit value(std::vector<bytes_opt> elements)
            : _elements(std::move(elements)) {
        }
        static value from_serialized(bytes_view v, shared_ptr<list_type_impl> type, serialization_format sf);
        virtual bytes_opt get(const query_options& options) override;
        virtual bytes get_with_protocol_version(serialization_format sf) override;
        bool equals(shared_ptr<list_type_impl> lt, const value& v);
        virtual std::vector<bytes_opt> get_elements() override;
        virtual sstring to_string() const;
        friend class lists;
    };
    /**
     * Basically similar to a Value, but with some non-pure function (that need
     * to be evaluated at execution time) in it.
     *
     * Note: this would also work for a list with bind markers, but we don't support
     * that because 1) it's not excessively useful and 2) we wouldn't have a good
     * column name to return in the ColumnSpecification for those markers (not a
     * blocker per-se but we don't bother due to 1)).
     */
    class delayed_value : public non_terminal {
        std::vector<shared_ptr<term>> _elements;
    public:
        explicit delayed_value(std::vector<shared_ptr<term>> elements)
                : _elements(std::move(elements)) {
        }
        virtual bool contains_bind_marker() const override;
        virtual void collect_marker_specification(shared_ptr<variable_specifications> bound_names);
        virtual shared_ptr<terminal> bind(const query_options& options) override;
    };

    /**
     * A marker for List values and IN relations
     */
    class marker : public abstract_marker {
    public:
        marker(int32_t bind_index, ::shared_ptr<column_specification> receiver)
            : abstract_marker{bind_index, std::move(receiver)}
        { }
#if 0
        protected Marker(int bindIndex, ColumnSpecification receiver)
        {
            super(bindIndex, receiver);
            assert receiver.type instanceof ListType;
        }
#endif
        virtual ::shared_ptr<terminal> bind(const query_options& options) override;
#if 0
        public Value bind(QueryOptions options) throws InvalidRequestException
        {
            ByteBuffer value = options.getValues().get(bindIndex);
            return value == null ? null : Value.fromSerialized(value, (ListType)receiver.type, options.getProtocolVersion());
        }
#endif
    };
#if 0
    /*
     * For prepend, we need to be able to generate unique but decreasing time
     * UUID, which is a bit challenging. To do that, given a time in milliseconds,
     * we adds a number representing the 100-nanoseconds precision and make sure
     * that within the same millisecond, that number is always decreasing. We
     * do rely on the fact that the user will only provide decreasing
     * milliseconds timestamp for that purpose.
     */
    private static class PrecisionTime
    {
        // Our reference time (1 jan 2010, 00:00:00) in milliseconds.
        private static final long REFERENCE_TIME = 1262304000000L;
        private static final AtomicReference<PrecisionTime> last = new AtomicReference<PrecisionTime>(new PrecisionTime(Long.MAX_VALUE, 0));

        public final long millis;
        public final int nanos;

        PrecisionTime(long millis, int nanos)
        {
            this.millis = millis;
            this.nanos = nanos;
        }

        static PrecisionTime getNext(long millis)
        {
            while (true)
            {
                PrecisionTime current = last.get();

                assert millis <= current.millis;
                PrecisionTime next = millis < current.millis
                    ? new PrecisionTime(millis, 9999)
                    : new PrecisionTime(millis, Math.max(0, current.nanos - 1));

                if (last.compareAndSet(current, next))
                    return next;
            }
        }
    }
#endif

    class setter : public operation {
    public:
        setter(const column_definition& column, shared_ptr<term> t)
                : operation(column, std::move(t)) {
        }
        virtual void execute(mutation& m, const exploded_clustering_prefix& prefix, const update_parameters& params) override;
    };

    class setter_by_index : public operation {
        shared_ptr<term> _idx;
    public:
        setter_by_index(const column_definition& column, shared_ptr<term> idx, shared_ptr<term> t)
            : operation(column, std::move(t)), _idx(std::move(idx)) {
        }
        virtual bool requires_read() override;
        virtual void collect_marker_specification(shared_ptr<variable_specifications> bound_names);
        virtual void execute(mutation& m, const exploded_clustering_prefix& prefix, const update_parameters& params) override;
    };

#if 0
    public static class Appender extends Operation
    {
        public Appender(ColumnDefinition column, Term t)
        {
            super(column, t);
        }

        public void execute(ByteBuffer rowKey, ColumnFamily cf, Composite prefix, UpdateParameters params) throws InvalidRequestException
        {
            assert column.type.isMultiCell() : "Attempted to append to a frozen list";
            doAppend(t, cf, prefix, column, params);
        }
    }
#endif

    static void do_append(shared_ptr<term> t,
            mutation& m,
            const exploded_clustering_prefix& prefix,
            const column_definition& column,
            const update_parameters& params,
            tombstone ts = {});

#if 0
    public static class Prepender extends Operation
    {
        public Prepender(ColumnDefinition column, Term t)
        {
            super(column, t);
        }

        public void execute(ByteBuffer rowKey, ColumnFamily cf, Composite prefix, UpdateParameters params) throws InvalidRequestException
        {
            assert column.type.isMultiCell() : "Attempted to prepend to a frozen list";
            Term.Terminal value = t.bind(params.options);
            if (value == null)
                return;

            assert value instanceof Lists.Value;
            long time = PrecisionTime.REFERENCE_TIME - (System.currentTimeMillis() - PrecisionTime.REFERENCE_TIME);

            List<ByteBuffer> toAdd = ((Lists.Value)value).elements;
            for (int i = 0; i < toAdd.size(); i++)
            {
                PrecisionTime pt = PrecisionTime.getNext(time);
                ByteBuffer uuid = ByteBuffer.wrap(UUIDGen.getTimeUUIDBytes(pt.millis, pt.nanos));
                cf.addColumn(params.makeColumn(cf.getComparator().create(prefix, column, uuid), toAdd.get(i)));
            }
        }
    }
#endif

    class discarder : public operation {
    public:
        discarder(const column_definition& column, shared_ptr<term> t)
                : operation(column, std::move(t)) {
        }
        virtual bool requires_read() override;
        virtual void execute(mutation& m, const exploded_clustering_prefix& prefix, const update_parameters& params) override;
    };

#if 0
    public static class DiscarderByIndex extends Operation
    {
        public DiscarderByIndex(ColumnDefinition column, Term idx)
        {
            super(column, idx);
        }

        @Override
        public boolean requiresRead()
        {
            return true;
        }

        public void execute(ByteBuffer rowKey, ColumnFamily cf, Composite prefix, UpdateParameters params) throws InvalidRequestException
        {
            assert column.type.isMultiCell() : "Attempted to delete an item by index from a frozen list";
            Term.Terminal index = t.bind(params.options);
            if (index == null)
                throw new InvalidRequestException("Invalid null value for list index");

            assert index instanceof Constants.Value;

            List<Cell> existingList = params.getPrefetchedList(rowKey, column.name);
            int idx = ByteBufferUtil.toInt(((Constants.Value)index).bytes);
            if (idx < 0 || idx >= existingList.size())
                throw new InvalidRequestException(String.format("List index %d out of bound, list has size %d", idx, existingList.size()));

            CellName elementName = existingList.get(idx).name();
            cf.addColumn(params.makeTombstone(elementName));
        }
    }
#endif
};

}

#endif
