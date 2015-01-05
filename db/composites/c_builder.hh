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
 * Copyright 2014 Cloudius Systems
 */

#pragma once

#include "database.hh"
#include "composite.hh"
#include <boost/any.hpp>

namespace db {
namespace composites {

/**
 * A builder of Composite.
 */
class c_builder
{
public:
    virtual int32_t remaining_count() = 0;

    virtual c_builder* add(bytes value) = 0;
    virtual c_builder* add(boost::any value) = 0;
    virtual std::unique_ptr<composite> build() = 0;
    virtual std::unique_ptr<composite> build_with(bytes value) = 0;
    virtual std::unique_ptr<composite> build_with(std::vector<bytes> values) = 0;
};

} // composites
} // db
