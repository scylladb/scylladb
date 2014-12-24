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
package org.apache.cassandra.cql3.statements;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import org.apache.cassandra.exceptions.InvalidRequestException;

import static org.apache.commons.lang3.ArrayUtils.EMPTY_OBJECT_ARRAY;

/**
 * Utility methods use to perform request validation.
 */
public final class RequestValidations
{
    /**
     * Checks that the specified expression is <code>true</code>. If not an <code>InvalidRequestException</code> will
     * be thrown.
     *
     * @param expression the expression to test
     * @param messageTemplate the error message
     * @param messageArgs the message arguments
     * @throws InvalidRequestException if the specified expression is <code>false</code>.
     */
    public static void checkTrue(boolean expression, String message) throws InvalidRequestException
    {
        checkTrue(expression, message, EMPTY_OBJECT_ARRAY);
    }

    /**
     * Checks that the specified expression is <code>true</code>. If not an <code>InvalidRequestException</code> will
     * be thrown.
     *
     * @param expression the expression to test
     * @param messageTemplate the template used to build the error message
     * @param messageArgs the message arguments
     * @throws InvalidRequestException if the specified expression is <code>false</code>.
     */
    public static void checkTrue(boolean expression,
                                 String messageTemplate,
                                 Object... messageArgs)
                                 throws InvalidRequestException
    {
        if (!expression)
            throw invalidRequest(messageTemplate, messageArgs);
    }

    /**
     * Checks that the specified list does not contains duplicates.
     *
     * @param list the list to test
     * @param messageTemplate the template used to build the error message
     * @param messageArgs the message arguments
     * @throws InvalidRequestException if the specified list contains duplicates.
     */
    public static void checkContainsNoDuplicates(List<?> list, String message) throws InvalidRequestException
    {
        if (new HashSet<>(list).size() != list.size())
            throw invalidRequest(message);
    }

    /**
     * Checks that the specified list contains only the specified elements.
     *
     * @param list the list to test
     * @param expectedElements the expected elements
     * @param message the error message
     * @throws InvalidRequestException if the specified list contains duplicates.
     */
    public static <E> void checkContainsOnly(List<E> list,
                                             List<E> expectedElements,
                                             String message) throws InvalidRequestException
    {
        List<E> copy = new ArrayList<>(list);
        copy.removeAll(expectedElements);
        if (!copy.isEmpty())
            throw invalidRequest(message);
    }

    /**
     * Checks that the specified expression is <code>false</code>. If not an <code>InvalidRequestException</code> will
     * be thrown.
     *
     * @param expression the expression to test
     * @param messageTemplate the template used to build the error message
     * @param messageArgs the message arguments
     * @throws InvalidRequestException if the specified expression is <code>true</code>.
     */
    public static void checkFalse(boolean expression,
                                  String messageTemplate,
                                  Object... messageArgs)
                                  throws InvalidRequestException
    {
        checkTrue(!expression, messageTemplate, messageArgs);
    }

    /**
     * Checks that the specified expression is <code>false</code>. If not an <code>InvalidRequestException</code> will
     * be thrown.
     *
     * @param expression the expression to test
     * @param message the error message
     * @throws InvalidRequestException if the specified expression is <code>true</code>.
     */
    public static void checkFalse(boolean expression, String message) throws InvalidRequestException
    {
        checkTrue(!expression, message);
    }

    /**
     * Checks that the specified object is NOT <code>null</code>.
     * If it is an <code>InvalidRequestException</code> will be throws.
     *
     * @param object the object to test
     * @param messageTemplate the template used to build the error message
     * @param messageArgs the message arguments
     * @return the object
     * @throws InvalidRequestException if the specified object is <code>null</code>.
     */
    public static <T> T checkNotNull(T object, String messageTemplate, Object... messageArgs)
            throws InvalidRequestException
    {
        checkTrue(object != null, messageTemplate, messageArgs);
        return object;
    }

    /**
     * Checks that the specified object is <code>null</code>.
     * If it is not an <code>InvalidRequestException</code> will be throws.
     *
     * @param object the object to test
     * @param messageTemplate the template used to build the error message
     * @param messageArgs the message arguments
     * @return the object
     * @throws InvalidRequestException if the specified object is not <code>null</code>.
     */
    public static <T> T checkNull(T object, String messageTemplate, Object... messageArgs)
            throws InvalidRequestException
    {
        checkTrue(object == null, messageTemplate, messageArgs);
        return object;
    }

    /**
     * Checks that the specified object is <code>null</code>.
     * If it is not an <code>InvalidRequestException</code> will be throws.
     *
     * @param object the object to test
     * @param message the error message
     * @return the object
     * @throws InvalidRequestException if the specified object is not <code>null</code>.
     */
    public static <T> T checkNull(T object, String message) throws InvalidRequestException
    {
        return checkNull(object, message, EMPTY_OBJECT_ARRAY);
    }

    /**
     * Returns an <code>InvalidRequestException</code> with the specified message.
     *
     * @param messageTemplate the template used to build the error message
     * @param messageArgs the message arguments
     * @return an <code>InvalidRequestException</code> with the specified message.
     */
    public static InvalidRequestException invalidRequest(String messageTemplate, Object... messageArgs)
    {
        return new InvalidRequestException(String.format(messageTemplate, messageArgs));
    }

    /**
     * This class must not be instantiated as it only contains static methods.
     */
    private RequestValidations()
    {

    }
}
