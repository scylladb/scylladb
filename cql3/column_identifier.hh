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

#ifndef CQL3_COLUMN_IDENTIFIER_HH
#define CQL3_COLUMN_IDENTIFIER_HH

#include "cql3/selection/selectable.hh"

#include "database.hh"

#include <algorithm>
#include <functional>

namespace cql3 {

#if 0
import java.util.List;
import java.util.Locale;
import java.nio.ByteBuffer;

import org.apache.cassandra.cache.IMeasurableMemory;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.cql3.selection.Selectable;
import org.apache.cassandra.cql3.selection.Selector;
import org.apache.cassandra.cql3.selection.SimpleSelector;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.memory.AbstractAllocator;
#endif

/**
 * Represents an identifer for a CQL column definition.
 * TODO : should support light-weight mode without text representation for when not interned
 */
class column_identifier final : public selection::selectable /* implements IMeasurableMemory*/ {
public:
    bytes bytes_;
private:
    sstring _text;
#if 0
    private static final long EMPTY_SIZE = ObjectSizes.measure(new ColumnIdentifier("", true));
#endif
public:
    column_identifier(sstring raw_text, bool keep_case) {
        _text = raw_text;
        if (!keep_case) {
            std::transform(_text.begin(), _text.end(), _text.begin(), ::tolower);
        }
        bytes_ = to_bytes(_text);
    }

#if 0
    public ColumnIdentifier(ByteBuffer bytes, AbstractType<?> type)
    {
        this.bytes = bytes;
        this.text = type.getString(bytes);
    }

    public ColumnIdentifier(ByteBuffer bytes, String text)
    {
        this.bytes = bytes;
        this.text = text;
    }
#endif

    bool operator==(const column_identifier& other) const {
        return bytes_ == other.bytes_;
    }

    sstring to_string() const {
        return _text;
    }

    friend std::ostream& operator<<(std::ostream& out, const column_identifier& i) {
        return out << i._text;
    }

#if 0
    public long unsharedHeapSize()
    {
        return EMPTY_SIZE
             + ObjectSizes.sizeOnHeapOf(bytes)
             + ObjectSizes.sizeOf(text);
    }

    public long unsharedHeapSizeExcludingData()
    {
        return EMPTY_SIZE
             + ObjectSizes.sizeOnHeapExcludingData(bytes)
             + ObjectSizes.sizeOf(text);
    }

    public ColumnIdentifier clone(AbstractAllocator allocator)
    {
        return new ColumnIdentifier(allocator.clone(bytes), text);
    }

    public Selector.Factory newSelectorFactory(CFMetaData cfm, List<ColumnDefinition> defs) throws InvalidRequestException
    {
        ColumnDefinition def = cfm.getColumnDefinition(this);
        if (def == null)
            throw new InvalidRequestException(String.format("Undefined name %s in selection clause", this));

        return SimpleSelector.newFactory(def.name.toString(), addAndGetIndex(def, defs), def.type);
    }
#endif

    /**
     * Because Thrift-created tables may have a non-text comparator, we cannot determine the proper 'key' until
     * we know the comparator. ColumnIdentifier.Raw is a placeholder that can be converted to a real ColumnIdentifier
     * once the comparator is known with prepare(). This should only be used with identifiers that are actual
     * column names. See CASSANDRA-8178 for more background.
     */
    class raw : public selectable::raw {
    private:
        const sstring _raw_text;
        sstring _text;
    public:
        raw(sstring raw_text, bool keep_case)
            : _raw_text{raw_text}
            , _text{raw_text}
        {
            if (!keep_case) {
                std::transform(_text.begin(), _text.end(), _text.begin(), ::tolower);
            }
        }

        virtual ::shared_ptr<selectable> prepare(schema_ptr s) override {
            return prepare_column_identifier(s);
        }

        ::shared_ptr<column_identifier> prepare_column_identifier(schema_ptr s) {
#if 0
            AbstractType<?> comparator = cfm.comparator.asAbstractType();
            if (cfm.getIsDense() || comparator instanceof CompositeType || comparator instanceof UTF8Type)
                return new ColumnIdentifier(text, true);

            // We have a Thrift-created table with a non-text comparator.  We need to parse column names with the comparator
            // to get the correct ByteBuffer representation.  However, this doesn't apply to key aliases, so we need to
            // make a special check for those and treat them normally.  See CASSANDRA-8178.
            ByteBuffer bufferName = ByteBufferUtil.bytes(text);
            for (ColumnDefinition def : cfm.partitionKeyColumns())
            {
                if (def.name.bytes.equals(bufferName))
                    return new ColumnIdentifier(text, true);
            }
            return new ColumnIdentifier(comparator.fromString(rawText), text);
#endif
            throw std::runtime_error("not implemented");
        }

        virtual bool processes_selection() const override {
            return false;
        }

#if 0
        @Override
        public final int hashCode()
        {
            return text.hashCode();
        }

        @Override
        public final boolean equals(Object o)
        {
            if(!(o instanceof ColumnIdentifier.Raw))
                return false;
            ColumnIdentifier.Raw that = (ColumnIdentifier.Raw)o;
            return text.equals(that.text);
        }

        @Override
        public String toString()
        {
            return text;
        }
#endif
    };
};

}

namespace std {

template<>
struct hash<cql3::column_identifier> {
    size_t operator()(const cql3::column_identifier& i) const {
        return std::hash<bytes>()(i.bytes_);
    }
};


}

#endif
