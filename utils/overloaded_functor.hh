/*
 * Copyright (C) 2018-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

/**
 * A helper class to encompass multiple lambdas accepting different input types
 * into a single object.
 */

template<typename... Ts> struct overloaded_functor : Ts... { using Ts::operator()...; };
template<typename... Ts> overloaded_functor(Ts...) -> overloaded_functor<Ts...>;

