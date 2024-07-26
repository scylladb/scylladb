/*
 * Modified by ScyllaDB
 *
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

#include "exceptions/exceptions.hh"
#include <seastar/core/print.hh>

#include <boost/range/algorithm/count_if.hpp>

#include <set>

namespace cql3 {
namespace statements {

/**
 * Utility methods use to perform request validation.
 */
namespace request_validations {

template <typename... MessageArgs>
exceptions::invalid_request_exception
invalid_request(fmt::format_string<MessageArgs...> message_template, MessageArgs&&... message_args);


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
                    fmt::format_string<MessageArgs...> message_template,
                    MessageArgs&&... message_args) {
        if (!expression) {
            throw exceptions::invalid_request_exception(fmt::format(message_template, std::forward<MessageArgs>(message_args)...));
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
    void check_contains_no_duplicates(const std::vector<T>& list, fmt::format_string<> message) {
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
                             fmt::format_string<> message) {
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
                     fmt::format_string<MessageArgs...> message_template,
                     MessageArgs&&... message_args) {
        check_true(!expression, message_template, std::forward<MessageArgs>(message_args)...);
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
    T check_not_null(T object, fmt::format_string<MessageArgs...> message_template, MessageArgs&... message_args) {
        check_true(bool(object), message_template, std::forward<MessageArgs>(message_args)...);
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
    T check_null(T object, fmt::format_string<MessageArgs...> message_template, MessageArgs&&... message_args) {
        check_true(!bool(object), message_template, std::forward<MessageArgs>(message_args)...);
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
    invalid_request(fmt::format_string<MessageArgs...> message_template, MessageArgs&&... message_args) {
        return exceptions::invalid_request_exception(fmt::format(message_template, std::forward<MessageArgs>(message_args)...));
    }
}

}
}
