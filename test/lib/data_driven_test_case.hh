/*
 * Copyright (C) 2015-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include <boost/preprocessor/seq/fold_left.hpp>
#include <boost/preprocessor/tuple/elem.hpp>
#include <boost/preprocessor/variadic/to_seq.hpp>
#include <boost/preprocessor/arithmetic/div.hpp>
#include <boost/preprocessor/arithmetic/inc.hpp>
#include <boost/preprocessor/arithmetic/mul.hpp>
#include <boost/preprocessor/control/expr_if.hpp>
#include <boost/preprocessor/punctuation/comma_if.hpp>
#include <boost/preprocessor/repetition/repeat.hpp>
#include <boost/preprocessor/tuple/elem.hpp>
#include <boost/preprocessor/variadic/size.hpp>


#define CONCAT_IMPL(a, b) a##b
#define CONCAT(a, b) CONCAT_IMPL(a, b)

#define APPEND_TOKEN(base, token)                               \
    CONCAT(CONCAT(base, _), token)
               
#define APPEND_PARAMETER(base, parameter, value)                \
    APPEND_TOKEN(APPEND_TOKEN(base, parameter), value)


#define MAKE_PAIR(z, index, arguments)                          \
BOOST_PP_COMMA_IF(index)(                                       \
    BOOST_PP_TUPLE_ELEM(BOOST_PP_MUL(2, index), arguments),     \
    BOOST_PP_TUPLE_ELEM(                                        \
        BOOST_PP_INC(BOOST_PP_MUL(2, index)), arguments))

#define MAKE_PAIRS(...)                                         \
    BOOST_PP_REPEAT(                                            \
        BOOST_PP_DIV(BOOST_PP_VARIADIC_SIZE(__VA_ARGS__), 2),   \
        MAKE_PAIR,                                              \
        (__VA_ARGS__))

#define TEST_NAME_FOLD_OP(s, result, pair)                      \
    APPEND_PARAMETER(                                           \
        result,                                                 \
        BOOST_PP_TUPLE_ELEM(2, 0, pair),                        \
        BOOST_PP_TUPLE_ELEM(2, 1, pair))

#define TEST_NAME(name, ...)                                    \
    BOOST_PP_SEQ_FOLD_LEFT(                                     \
        TEST_NAME_FOLD_OP,                                      \
        name,                                                   \
        BOOST_PP_VARIADIC_TO_SEQ(MAKE_PAIRS(__VA_ARGS__)))

#define EXTRACT_VALUE(z, index, arguments)                      \
    BOOST_PP_COMMA_IF(index)                                    \
    BOOST_PP_TUPLE_ELEM(                                        \
        BOOST_PP_INC(BOOST_PP_MUL(2, index)),                   \
        arguments)

#define EXTRACT_VALUES(...)                                     \
    BOOST_PP_REPEAT(                                            \
        BOOST_PP_DIV(BOOST_PP_VARIADIC_SIZE(__VA_ARGS__), 2),   \
        EXTRACT_VALUE,                                          \
        (__VA_ARGS__))

#define DATA_DRIVEN_TEST_CASE(func, ...)                        \
    SEASTAR_TEST_CASE(TEST_NAME(func, __VA_ARGS__)) {           \
        return func(EXTRACT_VALUES(__VA_ARGS__));               \
    }    