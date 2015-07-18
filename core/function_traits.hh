/*
 * This file is open source software, licensed to you under the terms
 * of the Apache License, Version 2.0 (the "License").  See the NOTICE file
 * distributed with this work for additional information regarding copyright
 * ownership.  You may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/*
 * Copyright 2015 Cloudius Systems
 */

#pragma once

#include <tuple>

template<typename T>
struct function_traits;
 
template<typename Ret, typename... Args>
struct function_traits<Ret(Args...)>
{
    using return_type = Ret;
    using args_as_tuple = std::tuple<Args...>;
    using signature = Ret (Args...);
 
    static constexpr std::size_t arity = sizeof...(Args);
 
    template <std::size_t N>
    struct arg
    {
        static_assert(N < arity, "no such parameter index.");
        using type = typename std::tuple_element<N, std::tuple<Args...>>::type;
    };
};

template<typename Ret, typename... Args>
struct function_traits<Ret(*)(Args...)> : public function_traits<Ret(Args...)>
{};

template <typename T, typename Ret, typename... Args>
struct function_traits<Ret(T::*)(Args...)> : public function_traits<Ret(Args...)>
{};

template <typename T, typename Ret, typename... Args>
struct function_traits<Ret(T::*)(Args...) const> : public function_traits<Ret(Args...)>
{};

template <typename T>
struct function_traits : public function_traits<decltype(&T::operator())>
{};

template<typename T>
struct function_traits<T&> : public function_traits<std::remove_reference_t<T>>
{};

