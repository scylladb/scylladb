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

#pragma once

#include <vector>
#include "core/sstring.hh"

namespace cql3 {

/**
 * Listener used to collect the syntax errors emitted by the Lexer and Parser.
 */
template<typename RecognizerType>
class error_listener {
public:
    /**
     * Invoked when a syntax error occurs.
     *
     * @param recognizer the parser or lexer that emitted the error
     * @param tokenNames the token names
     * @param e the exception
     */
    virtual void syntax_error(RecognizerType& recognizer, const std::vector<sstring>& token_names) = 0;

    /**
     * Invoked when a syntax error with a specified message occurs.
     *
     * @param recognizer the parser or lexer that emitted the error
     * @param errorMsg the error message
     */
    virtual void syntax_error(RecognizerType& recognizer, const sstring& error_msg) = 0;
};

}
