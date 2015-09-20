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
 *
 * Copyright 2015 Cloudius Systems
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

#include <cstdint>

namespace io {
class TypeSizes;

template<typename T>
class i_serializer {
public:
    /**
     * Serialize the specified type into the specified DataOutput instance.
     *
     *
     * @param t type that needs to be serialized
     * @param out DataOutput into which serialization needs to happen.
     * @throws java.io.IOException
     */
    virtual void serialize(T& t, DataOutputPlus out) = 0;

    /**
     * Deserialize from the specified DataInput instance.
     * @param in DataInput from which deserialization needs to happen.
     * @throws IOException
     * @return the type that was deserialized
     */
    virtual T& deserialize(DataInput in) = 0;

    virtual uint64_t serializedSize(T& t, TypeSizes type) = 0;
};

}
