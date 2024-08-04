/*
 * Copyright 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <concepts>
#include <type_traits>
#include "utils/assert.hh"
#include "utils/result.hh"

namespace utils {

namespace internal {

struct noop_converter {
    template<typename T>
    using wrapped_type = T;

    template<typename T>
    static
    wrapped_type<T>
    wrap(T&& t) {
        return std::move(t);
    }

    template<typename F, typename... Args>
    static
    wrapped_type<std::invoke_result_t<F, Args...>>
    invoke(const F& f, Args&&... args) {
        return std::invoke(f, std::forward<Args>(args)...);
    }
};

struct futurizing_converter {
    template<typename T>
    using wrapped_type = seastar::futurize_t<T>;

    template<typename T>
    static
    wrapped_type<T>
    wrap(T&& t) {
        if constexpr (seastar::is_future<T>::value) {
            return t;
        } else {
            return seastar::make_ready_future<T>(std::forward<T>(t));
        }
    }

    template<typename F, typename... Args>
    static
    wrapped_type<std::invoke_result_t<F, Args...>>
    invoke(const F& f, Args&&... args) {
        return seastar::futurize_invoke(f, std::forward<Args>(args)...);
    }
};

template<typename From, typename Converter, typename To>
concept ConvertsWithTo = std::convertible_to<typename Converter::template wrapped_type<From>, typename Converter::template wrapped_type<To>>;

// We require forall<ExceptionContainerResult R> ExceptionHandle<H, R>.
// However, C++ does not support quantification like that in the constraints.
// Here, we use the dummy_result to SCYLLA_ASSERT that the handles defined below
// at least work with dummy_result.
template<typename T = void>
using dummy_result = result_with_exception<T, std::exception>;

// TODO: Should we make a virtual base class for this?
// This would allow using non-generic lambdas in result_catch and result_catch_dots.
template<typename Handle>
concept ExceptionHandle =
    ExceptionContainerResult<dummy_result<>> &&
    requires (Handle handle, seastar::promise<dummy_result<int>>& p) {
        { handle.into_result() } -> std::same_as<dummy_result<>>;
        { handle.into_future() } -> std::same_as<seastar::future<dummy_result<>>>;
        { handle.forward_to_promise(p) } -> std::same_as<void>;
    };

template<ExceptionContainerResult R>
struct failed_result_handle {
private:
    R&& _failed_result;

public:
    failed_result_handle(R&& r) : _failed_result(std::move(r)) {}

    // Returns the failed result. Can be used only once as this moves out
    // the failed result. This should be called once during the handler's
    // lifetime, preferably as `return handler.into_result()`.
    R into_result() {
        return std::move(_failed_result);
    }

    seastar::future<R> into_future() {
        return seastar::make_ready_future<R>(std::move(_failed_result));
    }

    template<ExceptionContainerResult S>
    requires std::same_as<typename R::error_type, typename S::error_type>
    void forward_to_promise(seastar::promise<S>& p) {
        p.set_value(std::move(_failed_result).as_failure());
    }

    const typename R::error_type& as_inner() const & {
        return _failed_result.assume_error();
    }

    typename R::error_type&& as_inner() && {
        return std::move(_failed_result).assume_error();
    }

    typename R::error_type clone_inner() {
        return _failed_result.assume_error().clone();
    }
};

static_assert(ExceptionHandle<failed_result_handle<dummy_result<>>>);

template<ExceptionContainerResult R>
struct exception_ptr_handle {
private:
    std::exception_ptr _eptr;

public:
    exception_ptr_handle(std::exception_ptr&& eptr) : _eptr(std::move(eptr)) {}

    // Throws the exception.
    [[noreturn]] R into_result() {
        std::rethrow_exception(std::move(_eptr));
    }

    seastar::future<R> into_future() {
        return seastar::make_exception_future<R>(std::move(_eptr));
    }

    template<ExceptionContainerResult S>
    requires std::same_as<typename R::error_type, typename S::error_type>
    void forward_to_promise(seastar::promise<S>& p) {
        p.set_exception(std::move(_eptr));
    }

    const std::exception_ptr& as_inner() const & {
        return _eptr;
    }

    std::exception_ptr&& as_inner() && {
        return std::move(_eptr);
    }

    std::exception_ptr clone_inner() {
        return _eptr;
    }
};

// Check that it is a valid handle with some arbitrary ExceptionContainerResult
static_assert(ExceptionHandle<exception_ptr_handle<dummy_result<>>>);

template<typename Ex, typename Cb>
struct result_catcher {
private:
    Cb _cb;

public:
    using exception_type = Ex;
    using callback_type = Cb;

public:
    result_catcher(Cb&& cb) : _cb(std::move(cb)) {}

    template<ExceptionContainerResult R, typename Converter, typename Continuation>
    typename Converter::template wrapped_type<R>
    handle_exception_from_result(const auto& ex, R& original_result, Continuation&& cont) {
        if constexpr (std::is_base_of_v<Ex, std::remove_cvref_t<decltype(ex)>>) {
            if constexpr (std::is_invocable_v<Cb, const Ex&, failed_result_handle<R>>) {
                // Invoke with exception reference and handle
                return Converter::template invoke<Cb, const Ex&, failed_result_handle<R>>(_cb, ex, failed_result_handle<R>(std::move(original_result)));
            } else {
                // Simplified interface - invoke with reference to exception only
                static_assert(std::is_invocable_v<Cb, const Ex&>,
                        "The handler function does not have a suitable call operator");
                return Converter::template invoke<Cb, const Ex&>(_cb, ex);
            }
        } else {
            // Let another handler try it
            return std::invoke(std::forward<Continuation>(cont));
        }
    }

    template<ExceptionContainerResult R, typename Converter, typename Continuation>
    auto wrap_in_catch(Continuation&& cont) {
        return [this, cont = std::forward<Continuation>(cont)] (bool& already_caught) mutable -> typename Converter::template wrapped_type<R> {
            try {
                return std::invoke(std::forward<Continuation>(cont), already_caught);
            } catch (const Ex& ex) {
                if (already_caught) {
                    throw;
                }
                already_caught = true;

                if constexpr (std::is_invocable_v<Cb, const Ex&, exception_ptr_handle<R>>) {
                    // Invoke with exception reference and handle
                    return Converter::template invoke<const Cb, const Ex&, exception_ptr_handle<R>>(_cb, ex, exception_ptr_handle<R>(std::current_exception()));
                } else {
                    // Simplified interface - invoke with reference to exception only
                    static_assert(std::is_invocable_v<Cb, const Ex&>,
                            "The handler function does not have a suitable call operator");
                    return Converter::template invoke<const Cb, const Ex&>(_cb, ex);
                }
            }
        };
    }
};

template<typename T>
struct is_result_catcher : std::false_type {};

template<typename Ex, typename Cb>
struct is_result_catcher<result_catcher<Ex, Cb>> : std::true_type {};

template<typename Handler, typename Converter, typename Result>
concept ResultCatcherCallback =
    requires (typename Handler::callback_type callback, typename Handler::exception_type& ex,
            failed_result_handle<Result>&& frh, exception_ptr_handle<Result>&& eph) {
        { callback(ex, frh) } -> ConvertsWithTo<Converter, Result>;
        { callback(ex, eph) } -> ConvertsWithTo<Converter, Result>;
    } ||
    requires (typename Handler::callback_type callback, typename Handler::exception_type& ex) {
        { callback(ex) } -> ConvertsWithTo<Converter, Result>;
    };

template<typename Handler, typename Converter, typename Result>
concept ResultCatcher =
        is_result_catcher<Handler>::value &&
        ResultCatcherCallback<Handler, Converter, Result>;

template<typename Cb>
struct result_catcher_dots {
private:
    Cb _cb;

public:
    using callback_type = Cb;

public:
    result_catcher_dots(Cb&& cb) : _cb(std::move(cb)) {}

    template<ExceptionContainerResult R, typename Converter, typename Continuation>
    typename Converter::template wrapped_type<R>
    handle_exception_from_result(const auto& ex, R& original_result, Continuation&& cont) {
        if constexpr (std::is_invocable_v<Cb, failed_result_handle<R>>) {
            // Invoke with handle
            return Converter::template invoke<Cb, failed_result_handle<R>>(_cb, failed_result_handle<R>(std::move(original_result)));
        } else {
            // Simplified interface - invoke without arguments
            static_assert(std::is_invocable_v<Cb>,
                    "The handler function does not have a suitable call operator");
            return Converter::template invoke<Cb>(_cb);
        }
        // Don't propagate to the next handler. The catch (...) is supposed
        // to match on all errors.
    }

    template<ExceptionContainerResult R, typename Converter, typename Continuation>
    auto wrap_in_catch(Continuation&& cont) {
        return [this, cont = std::forward<Continuation>(cont)] (bool& already_caught) mutable -> typename Converter::template wrapped_type<R> {
            try {
                return std::invoke(std::forward<Continuation>(cont), already_caught);
            } catch (...) {
                if (already_caught) {
                    throw;
                }
                already_caught = true;

                if constexpr (std::is_invocable_v<Cb, exception_ptr_handle<R>>) {
                    // Invoke with handle
                    return Converter::template invoke<Cb, exception_ptr_handle<R>>(_cb, exception_ptr_handle<R>(std::current_exception()));
                } else {
                    // Simplified interface - invoke without arguments
                    static_assert(std::is_invocable_v<Cb>,
                            "The handler function does not have a suitable call operator");
                    return Converter::template invoke<Cb>(_cb);
                }
            }
        };
    }
};

template<typename T>
struct is_result_catcher_dots : std::false_type {};

template<typename Cb>
struct is_result_catcher_dots<result_catcher_dots<Cb>> : std::true_type {};

template<typename Handler, typename Converter, typename Result>
concept DotsResultCatcherCallback =
    requires (typename Handler::callback_type callback,
            failed_result_handle<Result>&& frh, exception_ptr_handle<Result>&& eph) {
        { callback(frh) } -> ConvertsWithTo<Converter, Result>;
        { callback(eph) } -> ConvertsWithTo<Converter, Result>;
    } ||
    requires (typename Handler::callback_type callback) {
        { callback() } -> ConvertsWithTo<Converter, Result>;
    };

template<typename Handler, typename Converter, typename Result>
concept DotsResultCatcher =
        is_result_catcher_dots<Handler>::value &&
        DotsResultCatcherCallback<Handler, Converter, Result>;

// Constructs an `invoke_in_try_catch` function which allows to call a callback
// and handle C++ exceptions using a set of handlers given during construction.
//
// The `Converter` is used to appropriately invoke the exception handlers.
template<ExceptionContainerResult R, typename Converter, typename... CatchHandlers>
struct try_catch_chain_impl {};

template<ExceptionContainerResult R, typename Converter, typename FirstCatchHandler, typename... CatchHandlers>
struct try_catch_chain_impl<R, Converter, FirstCatchHandler, CatchHandlers...> {
    template<typename Continuation>
    static
    typename Converter::template wrapped_type<R>
    invoke_in_try_catch(Continuation&& cont, FirstCatchHandler& first_handler, CatchHandlers&... catch_handlers) {
        return try_catch_chain_impl<R, Converter, CatchHandlers...>::template invoke_in_try_catch<>(
                first_handler.template wrap_in_catch<R, Converter, Continuation>(std::forward<Continuation>(cont)),
                catch_handlers...);
    }
};

template<ExceptionContainerResult R, typename Converter>
struct try_catch_chain_impl<R, Converter> {
    template<typename Continuation>
    static
    typename Converter::template wrapped_type<R>
    invoke_in_try_catch(Continuation&& cont) {
        // Not using an invoker as we always want it to throw
        bool already_caught = false;
        return std::invoke(std::forward<Continuation>(cont), already_caught);
    }
};

// Given a set of handlers, constructs a visitor which can be used to inspect
// a failed result.
//
// The `Converter` is used to appropriately invoke the exception handlers
// and convert the result to the return type if no handlers match on it.
template<ExceptionContainerResult R, typename Converter, typename... ResultHandlers>
struct combined_handler_impl {};

template<ExceptionContainerResult R, typename Converter, typename FirstResultHandler, typename... ResultHandlers>
struct combined_handler_impl<R, Converter, FirstResultHandler, ResultHandlers...>
        : protected combined_handler_impl<R, Converter, ResultHandlers...> {
private:
    using base = combined_handler_impl<R, Converter, ResultHandlers...>;
    FirstResultHandler& _first_handler;

public:
    combined_handler_impl(FirstResultHandler& first_handler, ResultHandlers&... result_handlers)
            : base(result_handlers...)
            , _first_handler(first_handler)
    { }

    typename Converter::template wrapped_type<R>
    handle(const auto& ex, R& original_result) {
        return _first_handler.template handle_exception_from_result<R, Converter>(
                ex, original_result,
                [this, &ex, &original_result] () mutable {
                    return base::handle(ex, original_result);
                });
    }

    auto with_original_result(R& res) {
        return [this, &res] (const auto& ex) {
            return this->handle(ex, res);
        };
    }
};

template<ExceptionContainerResult R, typename Converter>
struct combined_handler_impl<R, Converter> {
public:
    typename Converter::template wrapped_type<R>
    handle(const auto& ex, R& original_result) {
        // No more handlers to try out, just return
        return Converter::template wrap<R>(std::move(original_result));
    }

    auto with_original_result(R& res) {
        return [this, &res] (const auto& ex) {
            return handle(ex, res);
        };
    }
};

template<typename Result, typename... Handlers>
[[gnu::always_inline]]
inline
seastar::future<Result>
result_handle_available_future(seastar::future<Result>&& f, Handlers&... handlers) {
    using combined_handler_type = internal::combined_handler_impl<Result, internal::futurizing_converter, Handlers...>;
    using try_catch_chain_type = internal::try_catch_chain_impl<Result, internal::futurizing_converter, Handlers...>;

    if (!f.failed()) {
        Result&& res = f.get();
        if (res) {
            return seastar::make_ready_future<Result>(std::move(res));
        }

        // Handle the exception in result
        auto combined_handler = combined_handler_type(handlers...);
        return res.assume_error().accept(combined_handler.with_original_result(res));
    } else {
        // The future has an exception
        // We need to create a try..catch chain from the handlers
        // and rethrow the exception inside it
        //
        // futurize_invoke will protect us in case the exception is not handled
        // by any of the provided handlers.
        return seastar::futurize_invoke([&] {
            return try_catch_chain_type::invoke_in_try_catch(
                    [&f] (bool&) mutable -> seastar::future<Result> { std::rethrow_exception(std::move(f).get_exception()); },
                    handlers...);
        });
    }
}

}

template<typename Handler, typename Converter, typename Result>
concept ResultCatcherMaybeDots =
        internal::ResultCatcher<Handler, Converter, Result> ||
        internal::DotsResultCatcher<Handler, Converter, Result>;

template<typename Fun>
concept ResultTryBody = requires (Fun fun) {
    { fun() } -> ExceptionContainerResult<>;
} && std::is_nothrow_move_constructible_v<std::invoke_result_t<Fun>>;

template<typename Fun>
concept ResultFuturizeTryBody = requires (Fun&& fun) {
    { seastar::futurize_invoke(std::forward<Fun>(fun)) } -> ExceptionContainerResultFuture<>;
} && std::is_nothrow_move_constructible_v<typename seastar::futurize_t<std::invoke_result_t<Fun>>::value_type>;

/// \brief Allows to handle C++ exceptions and failed results in a unified way.
///
/// When you modify a code path to return a result<> and you encounter
/// a try..catch block, you can use it to migrate the block so that it handles
/// both C++ exceptions and exceptions stored in the result<>.
///
/// \section Example
///
/// Let's say that you have the following try..catch chain:
///
///   try {
///       return a_function_that_may_throw();
///   } catch (const my_exception& ex) {
///       return 123;
///   } catch (...) {
///       throw;
///   }
///
/// You can add support for results in the following way:
///
///   return utils::result_try([&] {
///       return a_function_that_may_throw_or_return_a_failed_result();
///   },  utils::result_catch<my_exception>([&] (const Ex&) -> result<int> {
///       return 123;
///   }), utils::result_catch_dots([&] (auto&& handle) -> result<int> {
///       return handle.into_result();
///   });
///
/// Each `result_catch` handler declares the exception type it handles
/// and accepts a const reference to it as the first argument.
/// The `result_catch_dots` handler is equivalent to `(...)` so it does not
/// declare exception type or accept a reference to it.
///
/// In addition, each `result_catch` and `result_catch_dots` can optionally
/// accept a reference to an exception handle. The exception handle can be used
/// to return/rethrow the exception being currently handled. Depending on
/// whether the handler is invoked for a C++ exception or a failed result<>,
/// it will have a different type and its `into_result()` method will either
/// rethrow the exception or return the failed result, respectively.
///
/// \section Limitations
///
/// The main limitation of result_try is that is is not as flexible with control
/// flow as a try..catch block is. If you have some logic after the try..catch
/// block and some code paths in it block return and some don't, you need to
/// mark it somehow in the return value - for example, you can change result<T>
/// to result<std::optional<T>> and continue if it is std::nullopt, otherwise
/// return.
///
/// The return type of the first argument must be nothrow move constructible.
/// Because this function uses many recursive calls to simulate a multi-catch
/// clause try..catch block and NRVO is not in general guaranteed, there is
/// a risk that some moves are not elided and one of them can throw
/// in the middle of the stack. This exception can be thrown after we already
/// exited the try..catch blocks for some handlers, so not all of them will
/// participate in handling this exception. Because of the inability to properly
/// handle exceptions from failed moves, hence the nothrow-move-constructibility
/// requirement.
///
/// This function does not work with futures and will trigger a static_assert
/// if you use it with a future-returning function. See result_futurize_try
/// for a version which additionally works with exceptional futures.
template<ResultTryBody Fun, ResultCatcherMaybeDots<internal::noop_converter, std::invoke_result_t<Fun>>... Handlers>
requires (!seastar::is_future<std::invoke_result_t<Fun>>::value)
inline
std::invoke_result_t<Fun>
result_try(Fun&& fun, Handlers&&... handlers) {
    using result_type = std::invoke_result_t<Fun>;
    using combined_handler_type = internal::combined_handler_impl<result_type, internal::noop_converter, Handlers...>;
    using try_catch_chain_type = internal::try_catch_chain_impl<result_type, internal::noop_converter, Handlers...>;

    // Invoke `fun` and catch C++ exceptions if any occur
    result_type res = try_catch_chain_type::template invoke_in_try_catch<>([&fun] (bool&) { return fun(); }, handlers...);
    if (res) {
        return res;
    }

    // No C++ exceptions but the result is a failure - inspect using a visitor
    auto combined_handler = combined_handler_type(handlers...);
    return res.assume_error().accept(combined_handler.with_original_result(res));
}

/// \brief A version of `result_try` which works with futures. It handles
/// C++ exceptions, failed results and exceptional futures returned from `fun`.
///
/// Migration from a try..catch block or f.handle_exception(...) is similar
/// as in the case of `result_try`, with a small number of differences
/// described below.
///
/// The `fun` function and all exception handlers are futurize_invoked,
/// therefore all C++ exceptions are converted to exceptional futures.
///
/// In order to perform `throw`/`make_exception_future<>(std::current_exception())`,
/// you should prefer `handle.into_future()` instead of `handle.into_result()`.
/// Unlike the latter, the former avoids rethrowing the exception and just
/// returns an exceptional future from a captured exception pointer.
///
/// Important note about preemption: if the `try` body returns a failure
/// without preemption, then the `catch` handler will be called without
/// preemption, too.
template<ResultFuturizeTryBody Fun,
         ResultCatcherMaybeDots<internal::futurizing_converter,
                                typename seastar::futurize_t<std::invoke_result_t<Fun>>::value_type>... Handlers>
seastar::futurize_t<std::invoke_result_t<Fun>>
result_futurize_try(Fun&& fun, Handlers&&... handlers) {
    using fun_return_type = std::invoke_result_t<Fun>;
    using future_type = seastar::futurize_t<fun_return_type>;
    using result_type = typename future_type::value_type;

    using try_catch_chain_type = internal::try_catch_chain_impl<result_type, internal::futurizing_converter, Handlers...>;

    // Try to invoke the function and handle C++ exceptions if there are any
    bool no_exceptions = false;
    future_type f = seastar::futurize_invoke([&] () {
        return try_catch_chain_type::template invoke_in_try_catch<>([&no_exceptions, &fun] (bool&) {
            // If `fun` didn't throw, we can set the `no_exceptions` flag
            // so that we know that there was a throw and we shouldn't
            // run the exception handling logic again.
            fun_return_type ret = fun();
            no_exceptions = true;
            return seastar::futurize<fun_return_type>::convert(std::move(ret));
        }, handlers...);
    });

    if (!__builtin_expect(no_exceptions, true)) {
        // There were C++ exceptions, and we handled them already
        return f;
    }

    // Although then_wrapped should call the lambda without preemption,
    // this is not guaranteed in debug mode, and I feel that the compiler
    // struggles to generate efficient code for this case in release mode.
    // We manually check if the future is available so that we can ensure
    // the guarantee stated in the comment.
    if (f.available()) {
        return internal::result_handle_available_future(std::move(f), handlers...);
    }
    return f.then_wrapped([...handlers = std::move(handlers)] (future_type&& f) mutable -> future_type {
        return internal::result_handle_available_future(std::move(f), handlers...);
    });
}

/// \brief Represents a `catch (const Ex& ex) {}` part in a `result_try` chain.
///
/// The callback must be a generic lambda, and be callable with one
/// of the following sets of arguments:
///
///    (const Ex&)
///    (const Ex&, Handle&&)
///
/// where `Handle` is satisfies the ExceptionHandle<R> concept
/// and `R` is the result return type.
///
/// See the description of `result_try` for more info.
template<typename Ex, typename Cb>
auto result_catch(Cb&& cb) {
    return internal::result_catcher<Ex, Cb>(std::move(cb));
}

/// \brief Represents a `catch (...) {}` part in a `result_try` chain.
///
/// The callback must be a generic lambda, and be callable with one
/// of the following sets of arguments:
///
///    ()
///    (Handle&&)
///
/// where `Handle` is satisfies the ExceptionHandle<R> concept
/// and `R` is the result return type.
///
/// See the description of `result_try` for more info.
template<typename Cb>
auto result_catch_dots(Cb&& cb) {
    return internal::result_catcher_dots<Cb>(std::move(cb));
}

}
