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
package org.apache.cassandra.exceptions;

import java.util.HashMap;
import java.util.Map;

import org.apache.cassandra.transport.ProtocolException;

/**
 * Exceptions code, as defined by the binary protocol.
 */
public enum ExceptionCode
{
    SERVER_ERROR    (0x0000),
    PROTOCOL_ERROR  (0x000A),

    BAD_CREDENTIALS (0x0100),

    // 1xx: problem during request execution
    UNAVAILABLE     (0x1000),
    OVERLOADED      (0x1001),
    IS_BOOTSTRAPPING(0x1002),
    TRUNCATE_ERROR  (0x1003),
    WRITE_TIMEOUT   (0x1100),
    READ_TIMEOUT    (0x1200),

    // 2xx: problem validating the request
    SYNTAX_ERROR    (0x2000),
    UNAUTHORIZED    (0x2100),
    INVALID         (0x2200),
    CONFIG_ERROR    (0x2300),
    ALREADY_EXISTS  (0x2400),
    UNPREPARED      (0x2500);

    public final int value;
    private static final Map<Integer, ExceptionCode> valueToCode = new HashMap<Integer, ExceptionCode>(ExceptionCode.values().length);
    static
    {
        for (ExceptionCode code : ExceptionCode.values())
            valueToCode.put(code.value, code);
    }

    private ExceptionCode(int value)
    {
        this.value = value;
    }

    public static ExceptionCode fromValue(int value)
    {
        ExceptionCode code = valueToCode.get(value);
        if (code == null)
            throw new ProtocolException(String.format("Unknown error code %d", value));
        return code;
    }
}
