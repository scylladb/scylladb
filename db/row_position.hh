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
#include "core/sstring.hh"
#include "util/serialization.hh"

#include "io/i_serializer.hh"

namespace db {

class IPartitioner;

class row_position /* FIXME: extends RingPosition<RowPosition> */ {
public:
    class row_position_serializer;
    class kind {
    public:
        // Only add new values to the end of the enum, the ordinal is used
        // during serialization
        static const kind ROW_KEY;
        static const kind MIN_BOUND;
        static const kind MAX_BOUND;
        kind(int8_t val) : _val(val) { }
        int8_t ordinal() const { return _val; }
        friend bool operator==(const kind& x, const kind& y) {
            return x._val == y._val;
        }
    private:
        int8_t _val;
    };

    class for_key {
    public:
#if 0
        static shared_ptr<row_position> get(bytes key, IPartitioner& p)
        {
            //return key == null || key.remaining() == 0 ? p.getMinimumToken().minKeyBound() : p.decorateKey(key);
            // FIXME: Implement IPartitioner and return the correct value
            return make_shared<row_position>();
        }
#endif
    };

    static std::unique_ptr<row_position_serializer> serializer;

    // Note: the orign name is kind() instead of get_kind(). We renamed it
    // since the function name conflicts with the class name.
    virtual row_position::kind get_kind() = 0;
    virtual bool is_minimum() = 0;

    class row_position_serializer /* FIXME: The signature has DataOutputPlus and DataInput : i_serializer<row_position> */ {
    public:
        /*
         * We need to be able to serialize both Token.KeyBound and
         * DecoratedKey. To make this compact, we first write a byte whose
         * meaning is:
         *   - 0: DecoratedKey
         *   - 1: a 'minimum' Token.KeyBound
         *   - 2: a 'maximum' Token.KeyBound
         * In the case of the DecoratedKey, we then serialize the key (the
         * token is recreated on the other side). In the other cases, we then
         * serialize the token.
         */
        void serialize(row_position& pos, std::ostream& out)
        {
            // FIXME: complete this func
            abort();
            const row_position::kind kind_ = pos.get_kind();
            serialize_int8(out, kind_.ordinal());
#if 0
            if (kind_ == row_position::kind::ROW_KEY)
                ByteBufferUtil.writeWithShortLength(((DecoratedKey)pos).getKey(), out);
            else
                Token.serializer.serialize(pos.getToken(), out);
#endif
        }
#if 0
        shared_ptr<row_position> deserialize(std::istream& in)
        {
            const row_position::kind kind_(deserialize_int8(in));
            if (kind_ == row_position::kind::ROW_KEY) {
                // bytes k = ByteBufferUtil.readWithShortLength(in);
                // return StorageService.getPartitioner().decorateKey(k);
            } else {
                // Token t = Token.serializer.deserialize(in);
                // return kind == kind.MIN_BOUND ? t.minKeyBound() : t.maxKeyBound();
            }
        }
#endif

        int64_t serialized_size(row_position& pos, TypeSizes& typeSizes)
        {
            // FIXME: complete this func
            abort();
            const row_position::kind kind_ = pos.get_kind();
            int size = 1; // 1 byte for enum
            if (kind_ == row_position::kind::ROW_KEY) {
#if 0
                int keySize = ((DecoratedKey)pos).getKey().remaining();
                size += typeSizes.sizeof((short) keySize) + keySize;
#endif
            } else {
#if 0
                size += Token.serializer.serializedSize(pos.getToken(), typeSizes);
#endif
            }
            return size;
        }
    };
};

} // db
