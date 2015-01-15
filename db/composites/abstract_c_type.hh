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
 * Modified by Cloudius Systems
 * Copyright 2015 Cloudius Systems
 */

#pragma once

#include "core/shared_ptr.hh"
#include "database.hh"
#include "db/composites/c_type.hh"
#include "db/composites/cell_name.hh"
#include "db/deletion_info.hh"
#include "db/range_tombstone.hh"

namespace db {
namespace composites {

// FIXME: stub
class index_info {
public:
    shared_ptr<composite> last_name;
    shared_ptr<composite> first_name;
};

// FIXME: stub
class column_slice {
public:
    class serializer {
    public:
        serializer(shared_ptr<c_type> type) {
           _type = type;
        }
    private:
        shared_ptr<c_type> _type;
    };
};

// FIXME: stub
class slice_query_filter {
public:
    class serializer {
    public:
        serializer(shared_ptr<c_type> type) {
           _type = type;
        }
    private:
        shared_ptr<c_type> _type;
    };
};

// FIXME: stub
class cell {
public:
    virtual shared_ptr<cell_name> name() const = 0;
};

// FIXME: stub
class native_cell : public cell {
public:
    virtual int32_t compare_to(shared_ptr<cell_name>) const {
        abort();
    }
    virtual int32_t compare_to(composite& c) const {
        abort();
    }
    virtual int32_t compare_to(shared_ptr<composite>) const {
        abort();
    }
};

class abstract_c_type : public c_type, public enable_shared_from_this<abstract_c_type> {
public:
    static thread_local comparator<cell> right_native_cell;
    static thread_local comparator<cell> neither_native_cell;
    // only one or the other of these will ever be used
    static thread_local comparator<shared_ptr<cell>> asymmetric_right_native_cell;
    static thread_local comparator<shared_ptr<cell>> asymmetric_neither_native_cell;

private:
    shared_ptr<comparator<composite>> _reverse_comparator;
    shared_ptr<comparator<index_info>> _index_comparator;
    shared_ptr<comparator<index_info>> _index_reverse_comparator;

    // FIXME
    // const Serializer serializer;

    shared_ptr<column_slice::serializer> _slice_serializer;
    shared_ptr<slice_query_filter::serializer> _slice_query_filter_serializer;
    shared_ptr<deletion_info::serializer> _deletion_info_serializer;
    shared_ptr<range_tombstone::serializer> _range_tombstone_serializer;

protected:
    bool _is_byte_order_comparable;

public:
    abstract_c_type(bool is_byte_order_comparable_) {
        _reverse_comparator = make_shared<comparator<composite>>(
            [this] (composite& c1, composite& c2) {
                return this->compare(c2, c1);
            }
        );
        _index_comparator = make_shared<comparator<index_info>>(
            [this] (index_info& o1, index_info& o2) {
                return this->compare(*(o1.last_name), *(o2.last_name));
            }
        );

        _index_reverse_comparator = make_shared<comparator<index_info>>(
            [this] (index_info& o1, index_info& o2) {
                return this->compare(*(o1.first_name), *(o2.first_name));
            }
        );


        // FIXME
        // serializer = new Serializer(this);

        shared_ptr<c_type> zis = dynamic_pointer_cast<c_type>(this->shared_from_this());
        _slice_serializer = make_shared<column_slice::serializer>(zis);
        _slice_query_filter_serializer = make_shared<slice_query_filter::serializer>(zis);
        _deletion_info_serializer = make_shared<deletion_info::serializer>(zis);
        _range_tombstone_serializer = make_shared<range_tombstone::serializer>(zis);

        this->_is_byte_order_comparable = is_byte_order_comparable_;
    }

    static bool is_byte_order_comparable(std::vector<abstract_type> types) {
        bool ret = true;
        for (auto& type : types)
            ret &= type.is_byte_order_comparable();
        return ret;
    }

    static int32_t compare_unsigned(composite& c1, composite& c2) {
        if (c1.is_static() != c2.is_static()) {
            // Static sorts before non-static no matter what, except for empty which
            // always sort first
            if (c1.is_empty())
                return c2.is_empty() ? 0 : -1;
            if (c2.is_empty())
                return 1;
            return c1.is_static() ? -1 : 1;
        }

        int32_t s1 = c1.size();
        int32_t s2 = c2.size();
        int32_t min_size = std::min(s1, s2);

        for (int32_t i = 0; i < min_size; i++) {
            int32_t cmp = ::compare_unsigned(c1.get(i), c2.get(i));
            if (cmp != 0)
                return cmp;
        }

        if (s1 == s2) {
            return c1.eoc().compare_to(c2.eoc());
        }
        return s1 < s2 ? c1.eoc().prefix_comparison_result : -c2.eoc().prefix_comparison_result;
    }

    int32_t compare(composite& c1, composite& c2)
    {
        if (c1.is_static() != c2.is_static()) {
            // Static sorts before non-static no matter what, except for empty which
            // always sort first
            if (c1.is_empty())
                return c2.is_empty() ? 0 : -1;
            if (c2.is_empty())
                return 1;
            return c1.is_static() ? -1 : 1;
        }

        int32_t s1 = c1.size();
        int32_t s2 = c2.size();
        int32_t min_size = std::min(s1, s2);

        for (int32_t i = 0; i < min_size; i++) {
            int32_t cmp = _is_byte_order_comparable
                      ? ::compare_unsigned(c1.get(i), c2.get(i))
                      : subtype(i)->compare(c1.get(i), c2.get(i));
            if (cmp != 0)
                return cmp;
        }

        if (s1 == s2)
            return c1.eoc().compare_to(c2.eoc());
        return s1 < s2 ? c1.eoc().prefix_comparison_result : -c2.eoc().prefix_comparison_result;
    }

    comparator<cell>& get_byte_order_column_comparator(bool is_right_native) {
        if (is_right_native)
            return right_native_cell;
        return neither_native_cell;
    }

    comparator<shared_ptr<cell>>& get_byte_order_asymmetric_column_comparator(bool is_right_native) {
        if (is_right_native)
            return asymmetric_right_native_cell;
        return asymmetric_neither_native_cell;
    }

    void validate(composite& name) {
        bytes previous = {};
        for (int32_t i = 0; i < name.size(); i++) {
            auto comparator = subtype(i);
            bytes value = name.get(i);
            comparator->validate_collection_member(value, previous);
            previous = value;
        }
    }

    bool is_compatible_with(c_type& previous) {
        // FIXME
        abort();
        /*
        if (*this == previous)
            return true;
        */

        // Extending with new components is fine, shrinking is not
        if (size() < previous.size())
            return false;

        for (int i = 0; i < previous.size(); i++) {
            auto tprev = previous.subtype(i);
            auto tnew = subtype(i);
            if (!tnew->is_compatible_with(*tprev))
                return false;
        }
        return true;
    }

#if 0
    public String getString(Composite c)
    {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < c.size(); i++)
        {
            if (i > 0)
                sb.append(":");
            sb.append(AbstractCompositeType.escape(subtype(i).getString(c.get(i))));
        }
        switch (c.eoc())
        {
            case START:
                sb.append(":_");
                break;
            case END:
                sb.append(":!");
                break;
        }
        return sb.toString();
    }

    public Composite make(Object... components)
    {
        if (components.length > size())
            throw new IllegalArgumentException("Too many components, max is " + size());

        CBuilder builder = builder();
        for (int i = 0; i < components.length; i++)
        {
            Object obj = components[i];
            if (obj instanceof ByteBuffer)
                builder.add((ByteBuffer)obj);
            else
                builder.add(obj);
        }
        return builder.build();
    }
#endif

    // public ctype::serializer serializer()
    // {
    //     return serializer;
    // }

    virtual shared_ptr<comparator<composite>> reverse_comparator() override {
        return _reverse_comparator;
    }
    virtual shared_ptr<comparator<index_info>> index_comparator() override {
        return _index_comparator;
    }

    virtual shared_ptr<comparator<index_info>> index_reverse_comparator() override {
        return _index_reverse_comparator;
    }

#if 0
    // NOTE: The origin code is IVersionedSerializer<ColumnSlice>
    public IVersionedSerializer<ColumnSlice> sliceSerializer() {
        return sliceSerializer;
    }

    public IVersionedSerializer<SliceQueryFilter> sliceQueryFilterSerializer() {
        return sliceQueryFilterSerializer;
    }
#endif

    virtual shared_ptr<deletion_info::serializer> deletion_info_serializer() override {
        return _deletion_info_serializer;
    }

    virtual shared_ptr<range_tombstone::serializer> range_tombstone_serializer() override {
        return _range_tombstone_serializer;
    }

#if 0

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;

        if (o == null)
            return false;

        if (!getClass().equals(o.getClass()))
            return false;

        CType c = (CType)o;
        if (size() != c.size())
            return false;

        for (int i = 0; i < size(); i++)
        {
            if (!subtype(i).equals(c.subtype(i)))
                return false;
        }
        return true;
    }

    @Override
    public int hashCode()
    {
        int h = 31;
        for (int i = 0; i < size(); i++)
            h += subtype(i).hashCode();
        return h + getClass().hashCode();
    }

    @Override
    public String toString()
    {
        return asAbstractType().toString();
    }

    protected static ByteBuffer sliceBytes(ByteBuffer bb, int offs, int length)
    {
        ByteBuffer copy = bb.duplicate();
        copy.position(offs);
        copy.limit(offs + length);
        return copy;
    }

    protected static void checkRemaining(ByteBuffer bb, int offs, int length)
    {
        if (offs + length > bb.limit())
            throw new IllegalArgumentException("Not enough bytes");
    }

    private static class Serializer implements CType.Serializer
    {
        private final CType type;

        public Serializer(CType type)
        {
            this.type = type;
        }

        public void serialize(Composite c, DataOutputPlus out) throws IOException
        {
            ByteBufferUtil.writeWithShortLength(c.toByteBuffer(), out);
        }

        public Composite deserialize(DataInput in) throws IOException
        {
            return type.fromByteBuffer(ByteBufferUtil.readWithShortLength(in));
        }

        public long serializedSize(Composite c, TypeSizes type)
        {
            return type.sizeofWithShortLength(c.toByteBuffer());
        }

        public void skip(DataInput in) throws IOException
        {
            ByteBufferUtil.skipShortLength(in);
        }
    }
#endif
};

}
}
