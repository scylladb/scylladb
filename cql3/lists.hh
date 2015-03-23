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
    static shared_ptr<column_specification> index_spec_of(shared_ptr<column_specification> column) {
        return make_shared<column_specification>(column->ks_name, column->cf_name,
                ::make_shared<column_identifier>(sprint("idx(%s)", *column->name), true), int32_type);
    }

    static shared_ptr<column_specification> value_spec_of(shared_ptr<column_specification> column) {
        return make_shared<column_specification>(column->ks_name, column->cf_name,
                ::make_shared<column_identifier>(sprint("value(%s)", *column->name), true),
                    dynamic_pointer_cast<list_type_impl>(column->type)->get_elements_type());
    }

    class literal : public term::raw {
        const std::vector<shared_ptr<term::raw>> _elements;
    public:
        explicit literal(std::vector<shared_ptr<term::raw>> elements)
            : _elements(std::move(elements)) {
        }

        shared_ptr<term> prepare(const sstring& keyspace, shared_ptr<column_specification> receiver) {
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
    private:
        void validate_assignable_to(const sstring keyspace, shared_ptr<column_specification> receiver) {
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
    public:
        virtual assignment_testable::test_result test_assignment(const sstring& keyspace, shared_ptr<column_specification> receiver) override {
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

        virtual sstring to_string() const override {
            return ::to_string(_elements);
        }
    };

    class value : public multi_item_terminal, collection_terminal {
        std::vector<bytes> _elements;
    public:
        explicit value(std::vector<bytes> elements)
            : _elements(std::move(elements)) {
        }

        static value from_serialized(bytes_view v, shared_ptr<list_type_impl> type, serialization_format sf) {
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

        virtual bytes_opt get(const query_options& options) override {
            return get_with_protocol_version(options.get_serialization_format());
        }

        virtual bytes get_with_protocol_version(serialization_format sf) override {
            return collection_type_impl::pack(_elements.begin(), _elements.end(), _elements.size(), sf);
        }

        bool equals(shared_ptr<list_type_impl> lt, const value& v) {
            if (_elements.size() != v._elements.size()) {
                return false;
            }
            return std::equal(_elements.begin(), _elements.end(),
                    v._elements.begin(),
                    [t = lt->get_elements_type()] (bytes_view e1, bytes_view e2) { return t->equal(e1, e2); });
        }

        virtual std::vector<bytes> get_elements() override {
            return _elements;
        }

        virtual sstring to_string() const {
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

        virtual bool contains_bind_marker() const override {
            // False since we don't support them in collection
            return false;
        }

        virtual void collect_marker_specification(shared_ptr<variable_specifications> bound_names) {
        }

        virtual shared_ptr<terminal> bind(const query_options& options) override {
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
        virtual ::shared_ptr<terminal> bind(const query_options& options) override {
            throw std::runtime_error("");
        }
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
        setter(column_definition& column, shared_ptr<term> t)
                : operation(column, std::move(t)) {
        }

        virtual void execute(mutation& m, const exploded_clustering_prefix& prefix, const update_parameters& params) override {
            if (column.type->is_multi_cell()) {
                // delete + append
                // FIXME:
                warn(unimplemented::cause::COLLECTIONS);
#if 0
                CellName name = cf.getComparator().create(prefix, column);
                cf.addAtom(params.makeTombstoneForOverwrite(name.slice()));
#endif
            }
            do_append(_t, m, prefix, column, params);
        }
    };

    class setter_by_index : public operation {
        shared_ptr<term> _idx;
    public:
        setter_by_index(column_definition& column, shared_ptr<term> idx, shared_ptr<term> t)
            : operation(column, std::move(t)), _idx(std::move(idx)) {
        }

        virtual bool requires_read() override {
            return true;
        }

        virtual void collect_marker_specification(shared_ptr<variable_specifications> bound_names) override {
            operation::collect_marker_specification(bound_names);
            _idx->collect_marker_specification(std::move(bound_names));
        }

        virtual void execute(mutation& m, const exploded_clustering_prefix& prefix, const update_parameters& params) override {
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
            const update_parameters& params) {
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

    public static class Discarder extends Operation
    {
        public Discarder(ColumnDefinition column, Term t)
        {
            super(column, t);
        }

        @Override
        public boolean requiresRead()
        {
            return true;
        }

        public void execute(ByteBuffer rowKey, ColumnFamily cf, Composite prefix, UpdateParameters params) throws InvalidRequestException
        {
            assert column.type.isMultiCell() : "Attempted to delete from a frozen list";
            List<Cell> existingList = params.getPrefetchedList(rowKey, column.name);
            // We want to call bind before possibly returning to reject queries where the value provided is not a list.
            Term.Terminal value = t.bind(params.options);

            if (existingList.isEmpty())
                return;

            if (value == null)
                return;

            assert value instanceof Lists.Value;

            // Note: below, we will call 'contains' on this toDiscard list for each element of existingList.
            // Meaning that if toDiscard is big, converting it to a HashSet might be more efficient. However,
            // the read-before-write this operation requires limits its usefulness on big lists, so in practice
            // toDiscard will be small and keeping a list will be more efficient.
            List<ByteBuffer> toDiscard = ((Lists.Value)value).elements;
            for (Cell cell : existingList)
            {
                if (toDiscard.contains(cell.value()))
                    cf.addColumn(params.makeTombstone(cell.name()));
            }
        }
    }

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
