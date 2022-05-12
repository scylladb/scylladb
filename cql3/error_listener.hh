/*
 * Copyright (C) 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
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
