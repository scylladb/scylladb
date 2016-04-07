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
 *
 * Modified by ScyllaDB
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

#pragma once

#include "cql3/term.hh"
#include "cql3/abstract_marker.hh"
#include "cql3/operator.hh"

namespace cql3 {

/**
 * A CQL3 condition on the value of a column or collection element.  For example, "UPDATE .. IF a = 0".
 */
class column_condition final {
public:
    const column_definition& column;
private:
    // For collection, when testing the equality of a specific element, nullptr otherwise.
    ::shared_ptr<term> _collection_element;
    ::shared_ptr<term> _value;
    std::vector<::shared_ptr<term>> _in_values;
    const operator_type& _op;
public:
    column_condition(const column_definition& column, ::shared_ptr<term> collection_element,
        ::shared_ptr<term> value, std::vector<::shared_ptr<term>> in_values, const operator_type& op)
            : column(column)
            , _collection_element(std::move(collection_element))
            , _value(std::move(value))
            , _in_values(std::move(in_values))
            , _op(op)
    {
        if (op != operator_type::IN) {
            assert(_in_values.empty());
        }
    }

    static ::shared_ptr<column_condition> condition(const column_definition& def, ::shared_ptr<term> value, const operator_type& op) {
        return ::make_shared<column_condition>(def, ::shared_ptr<term>{}, std::move(value), std::vector<::shared_ptr<term>>{}, op);
    }

    static ::shared_ptr<column_condition> condition(const column_definition& def, ::shared_ptr<term> collection_element,
            ::shared_ptr<term> value, const operator_type& op) {
        return ::make_shared<column_condition>(def, std::move(collection_element), std::move(value),
            std::vector<::shared_ptr<term>>{}, op);
    }

    static ::shared_ptr<column_condition> in_condition(const column_definition& def, std::vector<::shared_ptr<term>> in_values) {
        return ::make_shared<column_condition>(def, ::shared_ptr<term>{}, ::shared_ptr<term>{},
            std::move(in_values), operator_type::IN);
    }

    static ::shared_ptr<column_condition> in_condition(const column_definition& def, ::shared_ptr<term> collection_element,
            std::vector<::shared_ptr<term>> in_values) {
        return ::make_shared<column_condition>(def, std::move(collection_element), ::shared_ptr<term>{},
            std::move(in_values), operator_type::IN);
    }

    static ::shared_ptr<column_condition> in_condition(const column_definition& def, ::shared_ptr<term> in_marker) {
        return ::make_shared<column_condition>(def, ::shared_ptr<term>{}, std::move(in_marker),
            std::vector<::shared_ptr<term>>{}, operator_type::IN);
    }

    static ::shared_ptr<column_condition> in_condition(const column_definition& def, ::shared_ptr<term> collection_element,
        ::shared_ptr<term> in_marker) {
        return ::make_shared<column_condition>(def, std::move(collection_element), std::move(in_marker),
            std::vector<::shared_ptr<term>>{}, operator_type::IN);
    }

    bool uses_function(const sstring& ks_name, const sstring& function_name);
public:
    /**
     * Collects the column specification for the bind variables of this operation.
     *
     * @param boundNames the list of column specification where to collect the
     * bind variables of this term in.
     */
    void collect_marker_specificaton(::shared_ptr<variable_specifications> bound_names);

#if 0
    public ColumnCondition.Bound bind(QueryOptions options) throws InvalidRequestException
    {
        boolean isInCondition = operator == Operator.IN;
        if (column.type instanceof CollectionType)
        {
            if (collectionElement == null)
                return isInCondition ? new CollectionInBound(this, options) : new CollectionBound(this, options);
            else
                return isInCondition ? new ElementAccessInBound(this, options) : new ElementAccessBound(this, options);
        }
        return isInCondition ? new SimpleInBound(this, options) : new SimpleBound(this, options);
    }

    public static abstract class Bound
    {
        public final ColumnDefinition column;
        public final Operator operator;

        protected Bound(ColumnDefinition column, Operator operator)
        {
            this.column = column;
            this.operator = operator;
        }

        /**
         * Validates whether this condition applies to {@code current}.
         */
        public abstract boolean appliesTo(Composite rowPrefix, ColumnFamily current, long now) throws InvalidRequestException;

        public ByteBuffer getCollectionElementValue()
        {
            return null;
        }

        protected boolean isSatisfiedByValue(ByteBuffer value, Cell c, AbstractType<?> type, Operator operator, long now) throws InvalidRequestException
        {
            ByteBuffer columnValue = (c == null || !c.isLive(now)) ? null : c.value();
            return compareWithOperator(operator, type, value, columnValue);
        }

        /** Returns true if the operator is satisfied (i.e. "value operator otherValue == true"), false otherwise. */
        protected boolean compareWithOperator(Operator operator, AbstractType<?> type, ByteBuffer value, ByteBuffer otherValue) throws InvalidRequestException
        {
            if (value == null)
            {
                switch (operator)
                {
                    case EQ:
                        return otherValue == null;
                    case NEQ:
                        return otherValue != null;
                    default:
                        throw new InvalidRequestException(String.format("Invalid comparison with null for operator \"%s\"", operator));
                }
            }
            else if (otherValue == null)
            {
                // the condition value is not null, so only NEQ can return true
                return operator == Operator.NEQ;
            }
            int comparison = type.compare(otherValue, value);
            switch (operator)
            {
                case EQ:
                    return comparison == 0;
                case LT:
                    return comparison < 0;
                case LTE:
                    return comparison <= 0;
                case GT:
                    return comparison > 0;
                case GTE:
                    return comparison >= 0;
                case NEQ:
                    return comparison != 0;
                default:
                    // we shouldn't get IN, CONTAINS, or CONTAINS KEY here
                    throw new AssertionError();
            }
        }

        protected Iterator<Cell> collectionColumns(CellName collection, ColumnFamily cf, final long now)
        {
            // We are testing for collection equality, so we need to have the expected values *and* only those.
            ColumnSlice[] collectionSlice = new ColumnSlice[]{ collection.slice() };
            // Filter live columns, this makes things simpler afterwards
            return Iterators.filter(cf.iterator(collectionSlice), new Predicate<Cell>()
            {
                public boolean apply(Cell c)
                {
                    // we only care about live columns
                    return c.isLive(now);
                }
            });
        }
    }

    /**
     * A condition on a single non-collection column. This does not support IN operators (see SimpleInBound).
     */
    static class SimpleBound extends Bound
    {
        public final ByteBuffer value;

        private SimpleBound(ColumnCondition condition, QueryOptions options) throws InvalidRequestException
        {
            super(condition.column, condition.operator);
            assert !(column.type instanceof CollectionType) && condition.collectionElement == null;
            assert condition.operator != Operator.IN;
            this.value = condition.value.bindAndGet(options);
        }

        public boolean appliesTo(Composite rowPrefix, ColumnFamily current, long now) throws InvalidRequestException
        {
            CellName name = current.metadata().comparator.create(rowPrefix, column);
            return isSatisfiedByValue(value, current.getColumn(name), column.type, operator, now);
        }
    }

    /**
     * An IN condition on a single non-collection column.
     */
    static class SimpleInBound extends Bound
    {
        public final List<ByteBuffer> inValues;

        private SimpleInBound(ColumnCondition condition, QueryOptions options) throws InvalidRequestException
        {
            super(condition.column, condition.operator);
            assert !(column.type instanceof CollectionType) && condition.collectionElement == null;
            assert condition.operator == Operator.IN;
            if (condition.inValues == null)
                this.inValues = ((Lists.Marker) condition.value).bind(options).getElements();
            else
            {
                this.inValues = new ArrayList<>(condition.inValues.size());
                for (Term value : condition.inValues)
                    this.inValues.add(value.bindAndGet(options));
            }
        }

        public boolean appliesTo(Composite rowPrefix, ColumnFamily current, long now) throws InvalidRequestException
        {
            CellName name = current.metadata().comparator.create(rowPrefix, column);
            for (ByteBuffer value : inValues)
            {
                if (isSatisfiedByValue(value, current.getColumn(name), column.type, Operator.EQ, now))
                    return true;
            }
            return false;
        }
    }

    /** A condition on an element of a collection column. IN operators are not supported here, see ElementAccessInBound. */
    static class ElementAccessBound extends Bound
    {
        public final ByteBuffer collectionElement;
        public final ByteBuffer value;

        private ElementAccessBound(ColumnCondition condition, QueryOptions options) throws InvalidRequestException
        {
            super(condition.column, condition.operator);
            assert column.type instanceof CollectionType && condition.collectionElement != null;
            assert condition.operator != Operator.IN;
            this.collectionElement = condition.collectionElement.bindAndGet(options);
            this.value = condition.value.bindAndGet(options);
        }

        public boolean appliesTo(Composite rowPrefix, ColumnFamily current, final long now) throws InvalidRequestException
        {
            if (collectionElement == null)
                throw new InvalidRequestException("Invalid null value for " + (column.type instanceof MapType ? "map" : "list") + " element access");

            if (column.type instanceof MapType)
            {
                MapType mapType = (MapType) column.type;
                if (column.type.isMultiCell())
                {
                    Cell cell = current.getColumn(current.metadata().comparator.create(rowPrefix, column, collectionElement));
                    return isSatisfiedByValue(value, cell, mapType.getValuesType(), operator, now);
                }
                else
                {
                    Cell cell = current.getColumn(current.metadata().comparator.create(rowPrefix, column));
                    ByteBuffer mapElementValue = cell.isLive(now) ? mapType.getSerializer().getSerializedValue(cell.value(), collectionElement, mapType.getKeysType())
                                                                  : null;
                    return compareWithOperator(operator, mapType.getValuesType(), value, mapElementValue);
                }
            }

            // sets don't have element access, so it's a list
            ListType listType = (ListType) column.type;
            if (column.type.isMultiCell())
            {
                ByteBuffer columnValue = getListItem(
                        collectionColumns(current.metadata().comparator.create(rowPrefix, column), current, now),
                        getListIndex(collectionElement));
                return compareWithOperator(operator, listType.getElementsType(), value, columnValue);
            }
            else
            {
                Cell cell = current.getColumn(current.metadata().comparator.create(rowPrefix, column));
                ByteBuffer listElementValue = cell.isLive(now) ? listType.getSerializer().getElement(cell.value(), getListIndex(collectionElement))
                                                               : null;
                return compareWithOperator(operator, listType.getElementsType(), value, listElementValue);
            }
        }

        static int getListIndex(ByteBuffer collectionElement) throws InvalidRequestException
        {
            int idx = ByteBufferUtil.toInt(collectionElement);
            if (idx < 0)
                throw new InvalidRequestException(String.format("Invalid negative list index %d", idx));
            return idx;
        }

        static ByteBuffer getListItem(Iterator<Cell> iter, int index)
        {
            int adv = Iterators.advance(iter, index);
            if (adv == index && iter.hasNext())
                return iter.next().value();
            else
                return null;
        }

        public ByteBuffer getCollectionElementValue()
        {
            return collectionElement;
        }
    }

    static class ElementAccessInBound extends Bound
    {
        public final ByteBuffer collectionElement;
        public final List<ByteBuffer> inValues;

        private ElementAccessInBound(ColumnCondition condition, QueryOptions options) throws InvalidRequestException
        {
            super(condition.column, condition.operator);
            assert column.type instanceof CollectionType && condition.collectionElement != null;
            this.collectionElement = condition.collectionElement.bindAndGet(options);

            if (condition.inValues == null)
                this.inValues = ((Lists.Marker) condition.value).bind(options).getElements();
            else
            {
                this.inValues = new ArrayList<>(condition.inValues.size());
                for (Term value : condition.inValues)
                    this.inValues.add(value.bindAndGet(options));
            }
        }

        public boolean appliesTo(Composite rowPrefix, ColumnFamily current, final long now) throws InvalidRequestException
        {
            if (collectionElement == null)
                throw new InvalidRequestException("Invalid null value for " + (column.type instanceof MapType ? "map" : "list") + " element access");

            CellNameType nameType = current.metadata().comparator;
            if (column.type instanceof MapType)
            {
                MapType mapType = (MapType) column.type;
                AbstractType<?> valueType = mapType.getValuesType();
                if (column.type.isMultiCell())
                {
                    CellName name = nameType.create(rowPrefix, column, collectionElement);
                    Cell item = current.getColumn(name);
                    for (ByteBuffer value : inValues)
                    {
                        if (isSatisfiedByValue(value, item, valueType, Operator.EQ, now))
                            return true;
                    }
                    return false;
                }
                else
                {
                    Cell cell = current.getColumn(nameType.create(rowPrefix, column));
                    ByteBuffer mapElementValue  = null;
                    if (cell != null && cell.isLive(now))
                        mapElementValue =  mapType.getSerializer().getSerializedValue(cell.value(), collectionElement, mapType.getKeysType());
                    for (ByteBuffer value : inValues)
                    {
                        if (value == null)
                        {
                            if (mapElementValue == null)
                                return true;
                            continue;
                        }
                        if (valueType.compare(value, mapElementValue) == 0)
                            return true;
                    }
                    return false;
                }
            }

            ListType listType = (ListType) column.type;
            AbstractType<?> elementsType = listType.getElementsType();
            if (column.type.isMultiCell())
            {
                ByteBuffer columnValue = ElementAccessBound.getListItem(
                        collectionColumns(nameType.create(rowPrefix, column), current, now),
                        ElementAccessBound.getListIndex(collectionElement));

                for (ByteBuffer value : inValues)
                {
                    if (compareWithOperator(Operator.EQ, elementsType, value, columnValue))
                        return true;
                }
            }
            else
            {
                Cell cell = current.getColumn(nameType.create(rowPrefix, column));
                ByteBuffer listElementValue = null;
                if (cell != null && cell.isLive(now))
                    listElementValue = listType.getSerializer().getElement(cell.value(), ElementAccessBound.getListIndex(collectionElement));

                for (ByteBuffer value : inValues)
                {
                    if (value == null)
                    {
                        if (listElementValue == null)
                            return true;
                        continue;
                    }
                    if (elementsType.compare(value, listElementValue) == 0)
                        return true;
                }
            }
            return false;
        }
    }

    /** A condition on an entire collection column. IN operators are not supported here, see CollectionInBound. */
    static class CollectionBound extends Bound
    {
        private final Term.Terminal value;

        private CollectionBound(ColumnCondition condition, QueryOptions options) throws InvalidRequestException
        {
            super(condition.column, condition.operator);
            assert column.type.isCollection() && condition.collectionElement == null;
            assert condition.operator != Operator.IN;
            this.value = condition.value.bind(options);
        }

        public boolean appliesTo(Composite rowPrefix, ColumnFamily current, final long now) throws InvalidRequestException
        {
            CollectionType type = (CollectionType)column.type;

            if (type.isMultiCell())
            {
                Iterator<Cell> iter = collectionColumns(current.metadata().comparator.create(rowPrefix, column), current, now);
                if (value == null)
                {
                    if (operator == Operator.EQ)
                        return !iter.hasNext();
                    else if (operator == Operator.NEQ)
                        return iter.hasNext();
                    else
                        throw new InvalidRequestException(String.format("Invalid comparison with null for operator \"%s\"", operator));
                }

                return valueAppliesTo(type, iter, value, operator);
            }

            // frozen collections
            Cell cell = current.getColumn(current.metadata().comparator.create(rowPrefix, column));
            if (value == null)
            {
                if (operator == Operator.EQ)
                    return cell == null || !cell.isLive(now);
                else if (operator == Operator.NEQ)
                    return cell != null && cell.isLive(now);
                else
                    throw new InvalidRequestException(String.format("Invalid comparison with null for operator \"%s\"", operator));
            }

            // make sure we use v3 serialization format for comparison
            ByteBuffer conditionValue;
            if (type.kind == CollectionType.Kind.LIST)
                conditionValue = ((Lists.Value) value).getWithProtocolVersion(Server.VERSION_3);
            else if (type.kind == CollectionType.Kind.SET)
                conditionValue = ((Sets.Value) value).getWithProtocolVersion(Server.VERSION_3);
            else
                conditionValue = ((Maps.Value) value).getWithProtocolVersion(Server.VERSION_3);

            return compareWithOperator(operator, type, conditionValue, cell.value());
        }

        static boolean valueAppliesTo(CollectionType type, Iterator<Cell> iter, Term.Terminal value, Operator operator)
        {
            if (value == null)
                return !iter.hasNext();

            switch (type.kind)
            {
                case LIST: return listAppliesTo((ListType)type, iter, ((Lists.Value)value).elements, operator);
                case SET: return setAppliesTo((SetType)type, iter, ((Sets.Value)value).elements, operator);
                case MAP: return mapAppliesTo((MapType)type, iter, ((Maps.Value)value).map, operator);
            }
            throw new AssertionError();
        }

        private static boolean setOrListAppliesTo(AbstractType<?> type, Iterator<Cell> iter, Iterator<ByteBuffer> conditionIter, Operator operator, boolean isSet)
        {
            while(iter.hasNext())
            {
                if (!conditionIter.hasNext())
                    return (operator == Operator.GT) || (operator == Operator.GTE) || (operator == Operator.NEQ);

                // for lists we use the cell value; for sets we use the cell name
                ByteBuffer cellValue = isSet? iter.next().name().collectionElement() : iter.next().value();
                int comparison = type.compare(cellValue, conditionIter.next());
                if (comparison != 0)
                    return evaluateComparisonWithOperator(comparison, operator);
            }

            if (conditionIter.hasNext())
                return (operator == Operator.LT) || (operator == Operator.LTE) || (operator == Operator.NEQ);

            // they're equal
            return operator == Operator.EQ || operator == Operator.LTE || operator == Operator.GTE;
        }

        private static boolean evaluateComparisonWithOperator(int comparison, Operator operator)
        {
            // called when comparison != 0
            switch (operator)
            {
                case EQ:
                    return false;
                case LT:
                case LTE:
                    return comparison < 0;
                case GT:
                case GTE:
                    return comparison > 0;
                case NEQ:
                    return true;
                default:
                    throw new AssertionError();
            }
        }

        static boolean listAppliesTo(ListType type, Iterator<Cell> iter, List<ByteBuffer> elements, Operator operator)
        {
            return setOrListAppliesTo(type.getElementsType(), iter, elements.iterator(), operator, false);
        }

        static boolean setAppliesTo(SetType type, Iterator<Cell> iter, Set<ByteBuffer> elements, Operator operator)
        {
            ArrayList<ByteBuffer> sortedElements = new ArrayList<>(elements.size());
            sortedElements.addAll(elements);
            Collections.sort(sortedElements, type.getElementsType());
            return setOrListAppliesTo(type.getElementsType(), iter, sortedElements.iterator(), operator, true);
        }

        static boolean mapAppliesTo(MapType type, Iterator<Cell> iter, Map<ByteBuffer, ByteBuffer> elements, Operator operator)
        {
            Iterator<Map.Entry<ByteBuffer, ByteBuffer>> conditionIter = elements.entrySet().iterator();
            while(iter.hasNext())
            {
                if (!conditionIter.hasNext())
                    return (operator == Operator.GT) || (operator == Operator.GTE) || (operator == Operator.NEQ);

                Map.Entry<ByteBuffer, ByteBuffer> conditionEntry = conditionIter.next();
                Cell c = iter.next();

                // compare the keys
                int comparison = type.getKeysType().compare(c.name().collectionElement(), conditionEntry.getKey());
                if (comparison != 0)
                    return evaluateComparisonWithOperator(comparison, operator);

                // compare the values
                comparison = type.getValuesType().compare(c.value(), conditionEntry.getValue());
                if (comparison != 0)
                    return evaluateComparisonWithOperator(comparison, operator);
            }

            if (conditionIter.hasNext())
                return (operator == Operator.LT) || (operator == Operator.LTE) || (operator == Operator.NEQ);

            // they're equal
            return operator == Operator.EQ || operator == Operator.LTE || operator == Operator.GTE;
        }
    }

    public static class CollectionInBound extends Bound
    {
        private final List<Term.Terminal> inValues;

        private CollectionInBound(ColumnCondition condition, QueryOptions options) throws InvalidRequestException
        {
            super(condition.column, condition.operator);
            assert column.type instanceof CollectionType && condition.collectionElement == null;
            assert condition.operator == Operator.IN;
            inValues = new ArrayList<>();
            if (condition.inValues == null)
            {
                // We have a list of serialized collections that need to be deserialized for later comparisons
                CollectionType collectionType = (CollectionType) column.type;
                Lists.Marker inValuesMarker = (Lists.Marker) condition.value;
                if (column.type instanceof ListType)
                {
                    ListType deserializer = ListType.getInstance(collectionType.valueComparator(), false);
                    for (ByteBuffer buffer : inValuesMarker.bind(options).elements)
                    {
                        if (buffer == null)
                            this.inValues.add(null);
                        else
                            this.inValues.add(Lists.Value.fromSerialized(buffer, deserializer, options.getProtocolVersion()));
                    }
                }
                else if (column.type instanceof MapType)
                {
                    MapType deserializer = MapType.getInstance(collectionType.nameComparator(), collectionType.valueComparator(), false);
                    for (ByteBuffer buffer : inValuesMarker.bind(options).elements)
                    {
                        if (buffer == null)
                            this.inValues.add(null);
                        else
                            this.inValues.add(Maps.Value.fromSerialized(buffer, deserializer, options.getProtocolVersion()));
                    }
                }
                else if (column.type instanceof SetType)
                {
                    SetType deserializer = SetType.getInstance(collectionType.valueComparator(), false);
                    for (ByteBuffer buffer : inValuesMarker.bind(options).elements)
                    {
                        if (buffer == null)
                            this.inValues.add(null);
                        else
                            this.inValues.add(Sets.Value.fromSerialized(buffer, deserializer, options.getProtocolVersion()));
                    }
                }
            }
            else
            {
                for (Term value : condition.inValues)
                    this.inValues.add(value.bind(options));
            }
        }

        public boolean appliesTo(Composite rowPrefix, ColumnFamily current, final long now) throws InvalidRequestException
        {
            CollectionType type = (CollectionType)column.type;
            CellName name = current.metadata().comparator.create(rowPrefix, column);
            if (type.isMultiCell())
            {
                // copy iterator contents so that we can properly reuse them for each comparison with an IN value
                List<Cell> cells = newArrayList(collectionColumns(name, current, now));
                for (Term.Terminal value : inValues)
                {
                    if (CollectionBound.valueAppliesTo(type, cells.iterator(), value, Operator.EQ))
                        return true;
                }
                return false;
            }
            else
            {
                Cell cell = current.getColumn(name);
                for (Term.Terminal value : inValues)
                {
                    if (value == null)
                    {
                        if (cell == null || !cell.isLive(now))
                            return true;
                    }
                    else if (type.compare(((Term.CollectionTerminal)value).getWithProtocolVersion(Server.VERSION_3), cell.value()) == 0)
                    {
                        return true;
                    }
                }
                return false;
            }
        }
    }
#endif

    class raw final {
    private:
        ::shared_ptr<term::raw> _value;
        std::vector<::shared_ptr<term::raw>> _in_values;
        ::shared_ptr<abstract_marker::in_raw> _in_marker;

        // Can be nullptr, only used with the syntax "IF m[e] = ..." (in which case it's 'e')
        ::shared_ptr<term::raw> _collection_element;
        const operator_type& _op;
    public:
        raw(::shared_ptr<term::raw> value,
            std::vector<::shared_ptr<term::raw>> in_values,
            ::shared_ptr<abstract_marker::in_raw> in_marker,
            ::shared_ptr<term::raw> collection_element,
            const operator_type& op)
                : _value(std::move(value))
                , _in_values(std::move(in_values))
                , _in_marker(std::move(in_marker))
                , _collection_element(std::move(collection_element))
                , _op(op)
        { }

        /** A condition on a column. For example: "IF col = 'foo'" */
        static ::shared_ptr<raw> simple_condition(::shared_ptr<term::raw> value, const operator_type& op) {
            return ::make_shared<raw>(std::move(value), std::vector<::shared_ptr<term::raw>>{},
                ::shared_ptr<abstract_marker::in_raw>{}, ::shared_ptr<term::raw>{}, op);
        }

        /** An IN condition on a column. For example: "IF col IN ('foo', 'bar', ...)" */
        static ::shared_ptr<raw> simple_in_condition(std::vector<::shared_ptr<term::raw>> in_values) {
            return ::make_shared<raw>(::shared_ptr<term::raw>{}, std::move(in_values),
                ::shared_ptr<abstract_marker::in_raw>{}, ::shared_ptr<term::raw>{}, operator_type::IN);
        }

        /** An IN condition on a column with a single marker. For example: "IF col IN ?" */
        static ::shared_ptr<raw> simple_in_condition(::shared_ptr<abstract_marker::in_raw> in_marker) {
            return ::make_shared<raw>(::shared_ptr<term::raw>{}, std::vector<::shared_ptr<term::raw>>{},
                std::move(in_marker), ::shared_ptr<term::raw>{}, operator_type::IN);
        }

        /** A condition on a collection element. For example: "IF col['key'] = 'foo'" */
        static ::shared_ptr<raw> collection_condition(::shared_ptr<term::raw> value, ::shared_ptr<term::raw> collection_element,
                const operator_type& op) {
            return ::make_shared<raw>(std::move(value), std::vector<::shared_ptr<term::raw>>{}, ::shared_ptr<abstract_marker::in_raw>{}, std::move(collection_element), op);
        }

        /** An IN condition on a collection element. For example: "IF col['key'] IN ('foo', 'bar', ...)" */
        static ::shared_ptr<raw> collection_in_condition(::shared_ptr<term::raw> collection_element,
                std::vector<::shared_ptr<term::raw>> in_values) {
            return ::make_shared<raw>(::shared_ptr<term::raw>{}, std::move(in_values), ::shared_ptr<abstract_marker::in_raw>{},
                std::move(collection_element), operator_type::IN);
        }

        /** An IN condition on a collection element with a single marker. For example: "IF col['key'] IN ?" */
        static ::shared_ptr<raw> collection_in_condition(::shared_ptr<term::raw> collection_element,
                ::shared_ptr<abstract_marker::in_raw> in_marker) {
            return ::make_shared<raw>(::shared_ptr<term::raw>{}, std::vector<::shared_ptr<term::raw>>{}, std::move(in_marker),
                std::move(collection_element), operator_type::IN);
        }

        ::shared_ptr<column_condition> prepare(database& db, const sstring& keyspace, const column_definition& receiver);
    };
};

}
