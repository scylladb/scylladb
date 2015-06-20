/*
 * This file is open source software, licensed to you under the terms
 * of the Apache License, Version 2.0 (the "License").  See the NOTICE file
 * distributed with this work for additional information regarding copyright
 * ownership.  You may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/*
 * Copyright (C) 2015 Cloudius Systems, Ltd.
 */

#include "tests/test-utils.hh"

#include "core/distributed.hh"
#include "core/shared_ptr.hh"

SEASTAR_TEST_CASE(make_foreign_ptr_from_lw_shared_ptr) {
    auto p = make_foreign(make_lw_shared<sstring>("foo"));
    BOOST_REQUIRE(p->size() == 3);
    return make_ready_future<>();
}

SEASTAR_TEST_CASE(make_foreign_ptr_from_shared_ptr) {
    auto p = make_foreign(make_shared<sstring>("foo"));
    BOOST_REQUIRE(p->size() == 3);
    return make_ready_future<>();
}
