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
 * Modified by ScyllaDB
 *
 * Copyright (C) 2015 ScyllaDB
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

#include "exceptions/exceptions.hh"
#include "core/print.hh"

#include <boost/range/algorithm/count_if.hpp>

namespace cql3 {
namespace statements {

/**
 * Utility methods use to perform request validation.
 */
namespace request_validations {

template <typename... MessageArgs>
exceptions::invalid_request_exception
invalid_request(const char* message_template, const MessageArgs&... message_args);


    /**
     * Checks that the specified expression is <code>true</code>. If not an <code>InvalidRequestException</code> will
     * be thrown.
     *
     * @param expression the expression to test
     * @param message_template the template used to build the error message
     * @param message_args the message arguments
     * @throws InvalidRequestException if the specified expression is <code>false</code>.
     */
    template <typename... MessageArgs>
    void check_true(bool expression,
                    const char* message_template,
                    const MessageArgs&... message_args) {
        if (!expression) {
            throw exceptions::invalid_request_exception(sprint(message_template, message_args...));
        }
    }

    /**
     * Checks that the specified list does not contains duplicates.
     *
     * @param list the list to test
     * @param message The error message
     * @throws InvalidRequestException if the specified list contains duplicates.
     */
    template <typename T>
    void check_contains_no_duplicates(const std::vector<T>& list, const char* message) {
        if (std::set<T>(list.begin(), list.end()).size() != list.size()) {
            throw invalid_request(message);
        }
    }

    /**
     * Checks that the specified list contains only the specified elements.
     *
     * @param list the list to test
     * @param expected_elements the expected elements
     * @param message the error message
     * @throws InvalidRequestException if the specified list contains duplicates.
     */
    template <typename E>
    void check_contains_only(const std::vector<E>& list,
                             const std::vector<E>& expected_elements,
                             const char* message) {
        if (boost::count_if(list, [&] (const E& e) { return !boost::count_if(expected_elements, e); })) {
            throw invalid_request(message);
        }
    }

    /**
     * Checks that the specified expression is <code>false</code>. If not an <code>InvalidRequestException</code> will
     * be thrown.
     *
     * @param expression the expression to test
     * @param message_template the template used to build the error message
     * @param message_args the message arguments
     * @throws InvalidRequestException if the specified expression is <code>true</code>.
     */
    template <typename... MessageArgs>
    void check_false(bool expression,
                     const char* message_template,
                     const MessageArgs&... message_args) {
        check_true(!expression, message_template, message_args...);
    }

    /**
     * Checks that the specified object is NOT <code>null</code>.
     * If it is an <code>InvalidRequestException</code> will be throws.
     *
     * @param object the object to test
     * @param message_template the template used to build the error message
     * @param message_args the message arguments
     * @return the object
     * @throws InvalidRequestException if the specified object is <code>null</code>.
     */
    template <typename T, typename... MessageArgs>
    T check_not_null(T object, const char* message_template, const MessageArgs&... message_args) {
        check_true(bool(object), message_template, message_args...);
        return object;
    }

    /**
     * Checks that the specified object is <code>null</code>.
     * If it is not an <code>InvalidRequestException</code> will be throws.
     *
     * @param object the object to test
     * @param message_template the template used to build the error message
     * @param message_args the message arguments
     * @return the object
     * @throws InvalidRequestException if the specified object is not <code>null</code>.
     */
    template <typename T, typename... MessageArgs>
    T check_null(T object, const char* message_template, const MessageArgs&... message_args) {
        check_true(!bool(object), message_template, message_args...);
        return object;
    }

    /**
     * Returns an <code>InvalidRequestException</code> with the specified message.
     *
     * @param message_template the template used to build the error message
     * @param message_args the message arguments
     * @return an <code>InvalidRequestException</code> with the specified message.
     */
    template <typename... MessageArgs>
    exceptions::invalid_request_exception
    invalid_request(const char* message_template, const MessageArgs&... message_args) {
        return exceptions::invalid_request_exception(sprint(message_template, message_args...));
    }
}

}
}
