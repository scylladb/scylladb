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
 * Copyright (C) 2019-present ScyllaDB
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

#include "auth/sasl_challenge.hh"

#include "exceptions/exceptions.hh"

namespace auth {

/**
 * SASL PLAIN mechanism specifies that credentials are encoded in a
 * sequence of UTF-8 bytes, delimited by 0 (US-ASCII NUL).
 * The form is : {code}authzId<NUL>authnId<NUL>password<NUL>{code}
 * authzId is optional, and in fact we don't care about it here as we'll
 * set the authzId to match the authnId (that is, there is no concept of
 * a user being authorized to act on behalf of another).
 *
 * @param bytes encoded credentials string sent by the client
 * @return map containing the username/password pairs in the form an IAuthenticator
 * would expect
 * @throws javax.security.sasl.SaslException
 */
bytes plain_sasl_challenge::evaluate_response(bytes_view client_response) {
    sstring username, password;

    auto b = client_response.crbegin();
    auto e = client_response.crend();
    auto i = b;

    while (i != e) {
        if (*i == 0) {
            sstring tmp(i.base(), b.base());
            if (password.empty()) {
                password = std::move(tmp);
            } else if (username.empty()) {
                username = std::move(tmp);
            }
            b = ++i;
            continue;
        }
        ++i;
    }

    if (username.empty()) {
        throw exceptions::authentication_exception("Authentication ID must not be null");
    }
    if (password.empty()) {
        throw exceptions::authentication_exception("Password must not be null");
    }

    _username = std::move(username);
    _password = std::move(password);
    return {};
}

bool plain_sasl_challenge::is_complete() const {
    return _username && _password;
}

future<authenticated_user> plain_sasl_challenge::get_authenticated_user() const {
    return _when_complete(*_username, *_password);
}

}
