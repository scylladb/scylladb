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

#include "operator.hh"

namespace cql3 {

const operator_type operator_type::EQ(0, operator_type::EQ, "=");
const operator_type operator_type::LT(4, operator_type::GT, "<");
const operator_type operator_type::LTE(3, operator_type::GTE, "<=");
const operator_type operator_type::GTE(1, operator_type::LTE, ">=");
const operator_type operator_type::GT(2, operator_type::LT, ">");
const operator_type operator_type::IN(7, operator_type::IN, "IN");
const operator_type operator_type::CONTAINS(5, operator_type::CONTAINS, "CONTAINS");
const operator_type operator_type::CONTAINS_KEY(6, operator_type::CONTAINS_KEY, "CONTAINS_KEY");
const operator_type operator_type::NEQ(8, operator_type::NEQ, "!=");

}
