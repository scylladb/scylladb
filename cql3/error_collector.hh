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

#include "cql3/error_listener.hh"
#include "exceptions/exceptions.hh"
#include "types.hh"

namespace cql3 {

/**
 * <code>ErrorListener</code> that collect and enhance the errors send by the CQL lexer and parser.
 */
template<typename Recognizer>
class error_collector : public error_listener<Recognizer> {
    /**
     * The offset of the first token of the snippet.
     */
    static const int32_t FIRST_TOKEN_OFFSET = 10;

    /**
     * The offset of the last token of the snippet.
     */
    static const int32_t LAST_TOKEN_OFFSET = 2;

    /**
     * The CQL query.
     */
    const sstring_view _query;

    /**
     * The error messages.
     */
    std::vector<sstring> _error_msgs;
public:

    /**
     * Creates a new <code>ErrorCollector</code> instance to collect the syntax errors associated to the specified CQL
     * query.
     *
     * @param query the CQL query that will be parsed
     */
    error_collector(const sstring_view& query) : _query(query) {}

    virtual void syntax_error(Recognizer& recognizer, const std::vector<sstring>& token_names) override {
        // FIXME: stub
        syntax_error(recognizer, "Parsing failed, detailed description construction not implemented yet");
#if 0
        String hdr = recognizer.getErrorHeader(e);
        String msg = recognizer.getErrorMessage(e, tokenNames);

        StringBuilder builder = new StringBuilder().append(hdr)
                .append(' ')
                .append(msg);

        if (recognizer instanceof Parser)
            appendQuerySnippet((Parser) recognizer, builder);

        errorMsgs.add(builder.toString());
#endif
    }

    virtual void syntax_error(Recognizer& recognizer, const sstring& msg) override {
        _error_msgs.emplace_back(msg);
    }

    /**
     * Throws the first syntax error found by the lexer or the parser if it exists.
     *
     * @throws SyntaxException the syntax error.
     */
    void throw_first_syntax_error() {
        if (!_error_msgs.empty()) {
            throw exceptions::syntax_exception(_error_msgs[0]);
        }
    }

#if 0

    /**
     * Appends a query snippet to the message to help the user to understand the problem.
     *
     * @param parser the parser used to parse the query
     * @param builder the <code>StringBuilder</code> used to build the error message
     */
    private void appendQuerySnippet(Parser parser, StringBuilder builder)
    {
        TokenStream tokenStream = parser.getTokenStream();
        int index = tokenStream.index();
        int size = tokenStream.size();

        Token from = tokenStream.get(getSnippetFirstTokenIndex(index));
        Token to = tokenStream.get(getSnippetLastTokenIndex(index, size));
        Token offending = tokenStream.get(getOffendingTokenIndex(index, size));

        appendSnippet(builder, from, to, offending);
    }

    /**
     * Appends a query snippet to the message to help the user to understand the problem.
     *
     * @param from the first token to include within the snippet
     * @param to the last token to include within the snippet
     * @param offending the token which is responsible for the error
     */
    final void appendSnippet(StringBuilder builder,
                             Token from,
                             Token to,
                             Token offending)
    {
        if (!areTokensValid(from, to, offending))
            return;

        String[] lines = query.split("\n");

        boolean includeQueryStart = (from.getLine() == 1) && (from.getCharPositionInLine() == 0);
        boolean includeQueryEnd = (to.getLine() == lines.length)
                && (getLastCharPositionInLine(to) == lines[lines.length - 1].length());

        builder.append(" (");

        if (!includeQueryStart)
            builder.append("...");

        String toLine = lines[lineIndex(to)];
        int toEnd = getLastCharPositionInLine(to);
        lines[lineIndex(to)] = toEnd >= toLine.length() ? toLine : toLine.substring(0, toEnd);
        lines[lineIndex(offending)] = highlightToken(lines[lineIndex(offending)], offending);
        lines[lineIndex(from)] = lines[lineIndex(from)].substring(from.getCharPositionInLine());

        for (int i = lineIndex(from), m = lineIndex(to); i <= m; i++)
            builder.append(lines[i]);

        if (!includeQueryEnd)
            builder.append("...");

        builder.append(")");
    }

    /**
     * Checks if the specified tokens are valid.
     *
     * @param tokens the tokens to check
     * @return <code>true</code> if all the specified tokens are valid ones,
     * <code>false</code> otherwise.
     */
    private static boolean areTokensValid(Token... tokens)
    {
        for (Token token : tokens)
        {
            if (!isTokenValid(token))
                return false;
        }
        return true;
    }

    /**
     * Checks that the specified token is valid.
     *
     * @param token the token to check
     * @return <code>true</code> if it is considered as valid, <code>false</code> otherwise.
     */
    private static boolean isTokenValid(Token token)
    {
        return token.getLine() > 0 && token.getCharPositionInLine() >= 0;
    }

    /**
     * Returns the index of the offending token. <p>In the case where the offending token is an extra
     * character at the end, the index returned by the <code>TokenStream</code> might be after the last token.
     * To avoid that problem we need to make sure that the index of the offending token is a valid index 
     * (one for which a token exist).</p>
     *
     * @param index the token index returned by the <code>TokenStream</code>
     * @param size the <code>TokenStream</code> size
     * @return the valid index of the offending token
     */
    private static int getOffendingTokenIndex(int index, int size)
    {
        return Math.min(index, size - 1);
    }

    /**
     * Puts the specified token within square brackets.
     *
     * @param line the line containing the token
     * @param token the token to put within square brackets
     */
    private static String highlightToken(String line, Token token)
    {
        String newLine = insertChar(line, getLastCharPositionInLine(token), ']');
        return insertChar(newLine, token.getCharPositionInLine(), '[');
    }

    /**
     * Returns the index of the last character relative to the beginning of the line 0..n-1
     *
     * @param token the token
     * @return the index of the last character relative to the beginning of the line 0..n-1
     */
    private static int getLastCharPositionInLine(Token token)
    {
        return token.getCharPositionInLine() + getLength(token);
    }

    /**
     * Return the token length.
     *
     * @param token the token
     * @return the token length
     */
    private static int getLength(Token token)
    {
        return token.getText().length();
    }

    /**
     * Inserts a character at a given position within a <code>String</code>.
     *
     * @param s the <code>String</code> in which the character must be inserted
     * @param index the position where the character must be inserted
     * @param c the character to insert
     * @return the modified <code>String</code>
     */
    private static String insertChar(String s, int index, char c)
    {
        return new StringBuilder().append(s.substring(0, index))
                .append(c)
                .append(s.substring(index))
                .toString();
    }

    /**
     * Returns the index of the line number on which this token was matched; index=0..n-1
     *
     * @param token the token
     * @return the index of the line number on which this token was matched; index=0..n-1
     */
    private static int lineIndex(Token token)
    {
        return token.getLine() - 1;
    }

    /**
     * Returns the index of the last token which is part of the snippet.
     *
     * @param index the index of the token causing the error
     * @param size the total number of tokens
     * @return the index of the last token which is part of the snippet.
     */
    private static int getSnippetLastTokenIndex(int index, int size)
    {
        return Math.min(size - 1, index + LAST_TOKEN_OFFSET);
    }

    /**
     * Returns the index of the first token which is part of the snippet.
     *
     * @param index the index of the token causing the error
     * @return the index of the first token which is part of the snippet.
     */
    private static int getSnippetFirstTokenIndex(int index)
    {
        return Math.max(0, index - FIRST_TOKEN_OFFSET);
    }
#endif
};

}
