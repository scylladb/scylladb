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
package org.apache.cassandra.cql3;

import org.antlr.runtime.BaseRecognizer;
import org.antlr.runtime.RecognitionException;

/**
 * Listener used to collect the syntax errors emitted by the Lexer and Parser.
 */
public interface ErrorListener
{
    /**
     * Invoked when a syntax error occurs.
     *
     * @param recognizer the parser or lexer that emitted the error
     * @param tokenNames the token names
     * @param e the exception
     */
    void syntaxError(BaseRecognizer recognizer, String[] tokenNames, RecognitionException e);

    /**
     * Invoked when a syntax error with a specified message occurs.
     *
     * @param recognizer the parser or lexer that emitted the error
     * @param errorMsg the error message
     */
    void syntaxError(BaseRecognizer recognizer, String errorMsg);
}
