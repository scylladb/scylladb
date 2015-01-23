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

#include "thrift_validation.hh"

namespace thrift_validation {

void validate_key(schema_ptr schema_, const bytes& key) {
    throw std::runtime_error("NOT IMPLEMENTED");
#if 0
        if (key == null || key.remaining() == 0)
        {
            throw new org.apache.cassandra.exceptions.InvalidRequestException("Key may not be empty");
        }

        // check that key can be handled by FBUtilities.writeShortByteArray
        if (key.remaining() > FBUtilities.MAX_UNSIGNED_SHORT)
        {
            throw new org.apache.cassandra.exceptions.InvalidRequestException("Key length of " + key.remaining() +
                                                                              " is longer than maximum of " +
                                                                              FBUtilities.MAX_UNSIGNED_SHORT);
        }

        try
        {
            metadata.getKeyValidator().validate(key);
        }
        catch (MarshalException e)
        {
            throw new org.apache.cassandra.exceptions.InvalidRequestException(e.getMessage());
        }
#endif
}

}
