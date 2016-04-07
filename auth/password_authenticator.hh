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
 * Copyright (C) 2016 ScyllaDB
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

#include "authenticator.hh"

namespace auth {

class password_authenticator : public authenticator {
public:
    static const sstring PASSWORD_AUTHENTICATOR_NAME;

    password_authenticator();
    ~password_authenticator();

    future<> init();

    const sstring& class_name() const override;
    bool require_authentication() const override;
    option_set supported_options() const override;
    option_set alterable_options() const override;
    future<::shared_ptr<authenticated_user>> authenticate(const credentials_map& credentials) const throw(exceptions::authentication_exception) override;
    future<> create(sstring username, const option_map& options) throw(exceptions::request_validation_exception, exceptions::request_execution_exception) override;
    future<> alter(sstring username, const option_map& options) throw(exceptions::request_validation_exception, exceptions::request_execution_exception) override;
    future<> drop(sstring username) throw(exceptions::request_validation_exception, exceptions::request_execution_exception) override;
    resource_ids protected_resources() const override;
    ::shared_ptr<sasl_challenge> new_sasl_challenge() const override;


    static db::consistency_level consistency_for_user(const sstring& username);
};

}

