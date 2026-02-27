/*
 * Modified by ScyllaDB
 *
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (LicenseRef-ScyllaDB-Source-Available-1.0 and Apache-2.0)
 */

#pragma once

#include "exceptions/exceptions.hh"
#include <seastar/core/format.hh>

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
