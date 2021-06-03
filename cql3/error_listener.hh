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
 * Copyright (C) 2015-present ScyllaDB
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

#include "seastarx.hh"
#include <seastar/core/sstring.hh>
#include <antlr3.hpp>

namespace cql3 {

/**
 * Listener used to collect the syntax errors emitted by the Lexer and Parser.
 */
template<typename RecognizerType, typename ExceptionBaseType>
class error_listener {
public:
    virtual ~error_listener() = default;

    /**
     * Invoked when a syntax error occurs.
     *
     * @param recognizer the parser or lexer that emitted the error
     * @param tokenNames the token names
     * @param e the exception
     */
    virtual void syntax_error(RecognizerType& recognizer, ANTLR_UINT8** token_names, ExceptionBaseType* ex) = 0;

    /**
     * Invoked when a syntax error with a specified message occurs.
     *
     * @param recognizer the parser or lexer that emitted the error
     * @param errorMsg the error message
     */
    virtual void syntax_error(RecognizerType& recognizer, const sstring& error_msg) = 0;
};

}
