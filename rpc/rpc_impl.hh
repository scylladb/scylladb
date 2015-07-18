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
 * Copyright (C) 2015 Cloudius Systems, Ltd.
 */

#include <iostream>
#include "core/function_traits.hh"
#include "core/apply.hh"
#include "core/shared_ptr.hh"
#include "core/sstring.hh"
#include "core/future-util.hh"
#include "core/do_with.hh"
#include "util/is_smart_ptr.hh"

namespace rpc {

struct wait_type {}; // opposite of no_wait_type

// tags to tell whether we want a const client_info& parameter
struct do_want_client_info {};
struct dont_want_client_info {};

// An rpc signature, in the form signature<Ret (In0, In1, In2)>.
template <typename Function>
struct signature;

// General case
template <typename Ret, typename... In>
struct signature<Ret (In...)> {
    using ret_type = Ret;
    using arg_types = std::tuple<In...>;
    using clean = signature;
    using want_client_info = dont_want_client_info;
};

// Specialize 'clean' for handlers that receive client_info
template <typename Ret, typename... In>
struct signature<Ret (const client_info&, In...)> {
    using ret_type = Ret;
    using arg_types = std::tuple<In...>;
    using clean = signature<Ret (In...)>;
    using want_client_info = do_want_client_info;
};

template <typename T>
struct wait_signature {
    using type = wait_type;
    using cleaned_type = T;
};

template <typename... T>
struct wait_signature<future<T...>> {
    using type = wait_type;
    using cleaned_type = future<T...>;
};

template <>
struct wait_signature<no_wait_type> {
    using type = no_wait_type;
    using cleaned_type = void;
};

template <>
struct wait_signature<future<no_wait_type>> {
    using type = no_wait_type;
    using cleaned_type = future<void>;
};

template <typename T>
using wait_signature_t = typename wait_signature<T>::type;

template <typename... In>
inline
std::tuple<In...>
maybe_add_client_info(dont_want_client_info, const client_info& ci, std::tuple<In...>&& args) {
    return std::move(args);
}

template <typename... In>
inline
std::tuple<std::reference_wrapper<const client_info>, In...>
maybe_add_client_info(do_want_client_info, const client_info& ci, std::tuple<In...>&& args) {
    return std::tuple_cat(std::make_tuple(std::cref(ci)), std::move(args));
}

template <bool IsSmartPtr>
struct serialize_helper;

template <>
struct serialize_helper<false> {
    template <typename Serializer, typename Output, typename T>
    static inline void serialize(Serializer& serializer, Output& out, const T& t) {
        return serializer.write(out, t);
    }
};

template <>
struct serialize_helper<true> {
    template <typename Serializer, typename Output, typename T>
    static inline void serialize(Serializer& serializer, Output& out, const T& t) {
        return serializer.write(out, *t);
    }
};

template <typename Serializer, typename Output, typename T>
inline void marshall_one(Serializer& serializer, Output& out, const T& arg) {
    using serialize_helper_type = serialize_helper<is_smart_ptr<typename std::remove_reference<T>::type>::value>;
    serialize_helper_type::serialize(serializer, out, arg);
}

struct ignorer {
    template <typename... T>
    ignorer(T&&...) {}
};

template <typename Serializer, typename Output, typename... T>
inline void do_marshall(Serializer& serializer, Output& out, const T&... args) {
    // C++ guarantees that brace-initialization expressions are evaluted in order
    ignorer ignore{(marshall_one(serializer, out, args), 1)...};
}

class measuring_output_stream {
    size_t _size = 0;
public:
    void write(const char* data, size_t size) {
        _size += size;
    }
    size_t size() const {
        return _size;
    }
};

class simple_output_stream {
    char* _p;
public:
    simple_output_stream(sstring& s, size_t start) : _p(s.begin() + start) {}
    void write(const char* data, size_t size) {
        _p = std::copy_n(data, size, _p);
    }
};

template <typename Serializer, typename... T>
inline sstring marshall(Serializer& serializer, size_t head_space, const T&... args) {
    measuring_output_stream measure;
    do_marshall(serializer, measure, args...);
    sstring ret(sstring::initialized_later(), measure.size() + head_space);
    simple_output_stream out(ret, head_space);
    do_marshall(serializer, out, args...);
    return ret;
}

template <typename Serializer, typename Input>
inline std::tuple<> do_unmarshall(Serializer& serializer, Input& in) {
    return std::make_tuple();
}

template <typename Serializer, typename Input, typename T0, typename... Trest>
inline std::tuple<T0, Trest...> do_unmarshall(Serializer& serializer, Input& in) {
    // FIXME: something less recursive
    auto first = std::make_tuple(serializer.template read(in, type<T0>()));
    auto rest = do_unmarshall<Serializer, Input, Trest...>(serializer, in);
    return std::tuple_cat(std::move(first), std::move(rest));
}

class simple_input_stream {
    const char* _p;
    size_t _size;
public:
    simple_input_stream(const char* p, size_t size) : _p(p), _size(size) {}
    void read(char* p, size_t size) {
        if (size > _size) {
            throw error("buffer overflow");
        }
        std::copy_n(_p, size, p);
        _p += size;
        _size -= size;
    }
};

template <typename Serializer, typename... T>
inline std::tuple<T...> unmarshall(Serializer& serializer, temporary_buffer<char> input) {
    simple_input_stream in(input.get(), input.size());
    return do_unmarshall<Serializer, simple_input_stream, T...>(serializer, in);
}

template <typename Payload, typename... T>
struct rcv_reply_base  {
    bool done = false;
    promise<T...> p;
    template<typename... V>
    void set_value(V&&... v) {
        done = true;
        p.set_value(std::forward<V>(v)...);
    }
    ~rcv_reply_base() {
        if (!done) {
            p.set_exception(closed_error());
        }
    }
};

template<typename Serializer, typename MsgType, typename T>
struct rcv_reply : rcv_reply_base<T, T> {
    inline void get_reply(typename protocol<Serializer, MsgType>::client& dst, temporary_buffer<char> input) {
        this->set_value(unmarshall<Serializer, T>(dst.serializer(), std::move(input)));
    }
};

template<typename Serializer, typename MsgType, typename... T>
struct rcv_reply<Serializer, MsgType, future<T...>> : rcv_reply_base<std::tuple<T...>, T...> {
    inline void get_reply(typename protocol<Serializer, MsgType>::client& dst, temporary_buffer<char> input) {
        this->set_value(unmarshall<Serializer, T...>(dst.serializer(), std::move(input)));
    }
};

template<typename Serializer, typename MsgType>
struct rcv_reply<Serializer, MsgType, void> : rcv_reply_base<void, void> {
    inline void get_reply(typename protocol<Serializer, MsgType>::client& dst, temporary_buffer<char> input) {
        this->set_value();
    }
};

template<typename Serializer, typename MsgType>
struct rcv_reply<Serializer, MsgType, future<>> : rcv_reply<Serializer, MsgType, void> {};

template <typename Serializer, typename MsgType, typename Ret, typename... InArgs>
inline auto wait_for_reply(wait_type, typename protocol<Serializer, MsgType>::client& dst, id_type msg_id, future<> sent,
        signature<Ret (InArgs...)> sig) {
    sent.finally([]{}); // discard result or exception, this path does not need to wait for message to be send
    using reply_type = rcv_reply<Serializer, MsgType, Ret>;
    auto lambda = [] (reply_type& r, typename protocol<Serializer, MsgType>::client& dst, id_type msg_id, temporary_buffer<char> data) mutable {
        if (msg_id >= 0) {
            dst.get_stats_internal().replied++;
            return r.get_reply(dst, std::move(data));
        } else {
            dst.get_stats_internal().exception_received++;
            std::string ex_str(data.begin(), data.end());
            r.done = true;
            r.p.set_exception(std::runtime_error(ex_str));
        }
    };
    using handler_type = typename protocol<Serializer, MsgType>::client::template reply_handler<reply_type, decltype(lambda)>;
    auto r = std::make_unique<handler_type>(std::move(lambda));
    auto fut = r->reply.p.get_future();
    dst.wait_for_reply(msg_id, std::move(r));
    return fut;
}

template<typename Serializer, typename MsgType, typename... InArgs>
inline auto wait_for_reply(no_wait_type, typename protocol<Serializer, MsgType>::client& dst, id_type msg_id, future<>&& sent,
        signature<no_wait_type (InArgs...)> sig) {  // no_wait overload
    return std::move(sent);
}

// Returns lambda that can be used to send rpc messages.
// The lambda gets client connection and rpc parameters as arguments, marshalls them sends
// to a server and waits for a reply. After receiving reply it unmarshalls it and signal completion
// to a caller.
template<typename Serializer, typename MsgType, typename Ret, typename... InArgs>
auto send_helper(MsgType t, signature<Ret (InArgs...)> sig) {
    return [t, sig] (typename protocol<Serializer, MsgType>::client& dst, const InArgs&... args) {
        if (dst.error()) {
            using cleaned_ret_type = typename wait_signature<Ret>::cleaned_type;
            return futurize<cleaned_ret_type>::make_exception_future(closed_error());
        }

        // send message
        auto msg_id = dst.next_message_id();
        promise<> sent; // will be fulfilled when data is sent
        auto fsent = sent.get_future();
        dst.get_stats_internal().pending++;
        sstring data = marshall(dst.serializer(), 24, args...);
        auto p = data.begin();
        *unaligned_cast<uint64_t*>(p) = net::hton(uint64_t(t));
        *unaligned_cast<int64_t*>(p + 8) = net::hton(msg_id);
        *unaligned_cast<uint64_t*>(p + 16) = net::hton(data.size() - 24);
        dst.out_ready() = do_with(std::move(data), [&dst] (sstring& data) {
            return dst.out_ready().then([&dst, &data] () mutable {
                return dst.out().write(data).then([&dst] {
                    return dst.out().flush();
                });
            });
        }).finally([&dst, sent = std::move(sent)] () mutable {
            dst.get_stats_internal().pending--;
            dst.get_stats_internal().sent_messages++;
            sent.set_value();
        });

        // prepare reply handler, if return type is now_wait_type this does nothing, since no reply will be sent
        using wait = wait_signature_t<Ret>;
        return wait_for_reply<Serializer, MsgType>(wait(), dst, msg_id, std::move(fsent), sig);
    };
}

template <typename Serializer, typename MsgType>
inline
future<>
protocol<Serializer, MsgType>::server::connection::respond(int64_t msg_id, sstring&& data) {
    auto p = data.begin();
    *unaligned_cast<int64_t*>(p) = net::hton(msg_id);
    *unaligned_cast<uint64_t*>(p + 8) = net::hton(data.size() - 16);
    return do_with(std::move(data), [this, msg_id] (const sstring& data) {
        return this->out().write(data.begin(), data.size()).then([this] {
            return this->out().flush();
        });
    });
}

template<typename Serializer, typename MsgType, typename... RetTypes>
inline future<> reply(wait_type, future<RetTypes...>&& r, int64_t msgid, typename protocol<Serializer, MsgType>::server::connection& client) {
    try {
        auto&& data = r.get();
        auto str = ::apply(marshall<Serializer, const RetTypes&...>,
                std::tuple_cat(std::make_tuple(std::ref(client.serializer()), 16), std::move(data)));
        return client.respond(msgid, std::move(str));
    } catch (std::exception& ex) {
        return client.respond(-msgid, sstring(sstring::initialized_later(), 16) + ex.what());
    }
}

// specialization for no_wait_type which does not send a reply
template<typename Serializer, typename MsgType>
inline future<> reply(no_wait_type, future<no_wait_type>&& r, int64_t msgid, typename protocol<Serializer, MsgType>::server::connection& client) {
    try {
        r.get();
    } catch (std::exception& ex) {
        client.get_protocol().log(client.info(), msgid, to_sstring("exception \"") + ex.what() + "\" in no_wait handler ignored");
    }
    return make_ready_future<>();
}

template<typename Ret, typename... InArgs, typename WantClientInfo, typename Func, typename ArgsTuple>
inline futurize_t<Ret> apply(Func& func, client_info& info, WantClientInfo wci, signature<Ret (InArgs...)> sig, ArgsTuple&& args) {
    using futurator = futurize<Ret>;
    try {
        return futurator::apply(func, maybe_add_client_info(wci, info, std::forward<ArgsTuple>(args)));
    } catch (std::runtime_error& ex) {
        return futurator::make_exception_future(std::current_exception());
    }
}

// lref_to_cref is a helper that encapsulates lvalue reference in std::ref() or does nothing otherwise
template<typename T>
auto lref_to_cref(T&& x) {
    return std::move(x);
}

template<typename T>
auto lref_to_cref(T& x) {
    return std::ref(x);
}

// Creates lambda to handle RPC message on a server.
// The lambda unmarshalls all parameters, calls a handler, marshall return values and sends them back to a client
template <typename Serializer, typename MsgType, typename Func, typename Ret, typename... InArgs, typename WantClientInfo>
auto recv_helper(signature<Ret (InArgs...)> sig, Func&& func, WantClientInfo wci) {
    using signature = decltype(sig);
    using wait_style = wait_signature_t<Ret>;
    return [func = lref_to_cref(std::forward<Func>(func))](lw_shared_ptr<typename protocol<Serializer, MsgType>::server::connection> client,
                                                           int64_t msg_id,
                                                           temporary_buffer<char> data) mutable {
        auto args = unmarshall<Serializer, InArgs...>(client->serializer(), std::move(data));
        // note: apply is executed asynchronously with regards to networking so we cannot chain futures here by doing "return apply()"
        apply(func, client->info(), WantClientInfo(), signature(), std::move(args)).then_wrapped(
                [client, msg_id] (futurize_t<typename signature::ret_type> ret) {
            client->out_ready() = client->out_ready().then([client, msg_id, ret = std::move(ret)] () mutable {
                auto f = reply<Serializer, MsgType>(wait_style(), std::move(ret), msg_id, *client);
            });
        });
    };
}

// helper to create copy constructible lambda from non copy constructible one. std::function<> works only with former kind.
template<typename Func>
auto make_copyable_function(Func&& func, std::enable_if_t<!std::is_copy_constructible<std::decay_t<Func>>::value, void*> = nullptr) {
  auto p = make_lw_shared<typename std::decay_t<Func>>(std::forward<Func>(func));
  return [p] (auto&&... args) { return (*p)( std::forward<decltype(args)>(args)... ); };
}

template<typename Func>
auto make_copyable_function(Func&& func, std::enable_if_t<std::is_copy_constructible<std::decay_t<Func>>::value, void*> = nullptr) {
    return std::forward<Func>(func);
}

template<typename Ret, typename... Args>
struct handler_type_helper {
    using type = Ret(Args...);
    static constexpr bool info = false;
};

template<typename Ret, typename First, typename... Args>
struct handler_type_helper<Ret, First, Args...> {
    using type = Ret(First, Args...);
    static constexpr bool info = false;
    static_assert(!std::is_same<client_info&, First>::value, "reference to client_info has to be const");
};

template<typename Ret, typename... Args>
struct handler_type_helper<Ret, const client_info&, Args...> {
    using type = Ret(Args...);
    static constexpr bool info = true;
};

template<typename Ret, typename... Args>
struct handler_type_helper<Ret, client_info, Args...> {
    using type = Ret(Args...);
    static constexpr bool info = true;
};

template<typename Ret, typename F, typename I>
struct handler_type_impl;

template<typename Ret, typename F, std::size_t... I>
struct handler_type_impl<Ret, F, std::integer_sequence<std::size_t, I...>> {
    using type = handler_type_helper<Ret, typename F::template arg<I>::type...>;
};

// this class is used to calculate client side rpc function signature
// if rpc callback receives client_info as a first parameter it is dropped
// from an argument list and if return type is a smart pointer it is converted to be
// a type it points to, otherwise signature is identical to what was passed to
// make_client().
//
// Examples:
// std::unique_ptr<int>(client_info, int, long) -> int(int, long)
// double(client_info, float) -> double(float)
template<typename Func>
class client_function_type {
    template<typename T, bool IsSmartPtr>
    struct drop_smart_ptr_impl;
    template<typename T>
    struct drop_smart_ptr_impl<T, true> {
        using type = typename T::element_type;
    };
    template<typename T>
    struct drop_smart_ptr_impl<T, false> {
        using type = T;
    };
    template<typename T>
    using drop_smart_ptr = drop_smart_ptr_impl<T, is_smart_ptr<T>::value>;

    using trait = function_traits<Func>;
    // if return type is smart ptr take a type it points to instead
    using return_type = typename drop_smart_ptr<typename trait::return_type>::type;
public:
    using type = typename handler_type_impl<return_type, trait, std::make_index_sequence<trait::arity>>::type::type;
};

// this class is used to calculate client side rpc function signature
// if rpc callback receives client_info as a first parameter it is dropped
// from an argument list, otherwise signature is identical to what was passed to
// make_client().
template<typename Func>
class server_function_type {
    using trait = function_traits<Func>;
    using return_type = typename trait::return_type;
    using stype = typename handler_type_impl<return_type, trait, std::make_index_sequence<trait::arity>>::type;
public:
    using type = typename stype::type; // server function signature
    static constexpr bool info = stype::info; // true if client_info is a first parameter of rpc handler
};

template<typename Serializer, typename MsgType>
template<typename Func>
auto protocol<Serializer, MsgType>::make_client(MsgType t) {
    using trait = function_traits<typename client_function_type<Func>::type>;
    using sig_type = signature<typename trait::signature>;
    return send_helper<Serializer>(t, sig_type());
}

template<typename Serializer, typename MsgType>
template<typename Func>
auto protocol<Serializer, MsgType>::register_handler(MsgType t, Func&& func) {
    using sig_type = signature<typename function_traits<Func>::signature>;
    using clean_sig_type = typename sig_type::clean;
    using want_client_info = typename sig_type::want_client_info;
    auto recv = recv_helper<Serializer, MsgType>(clean_sig_type(), std::forward<Func>(func),
            want_client_info());
    register_receiver(t, make_copyable_function(std::move(recv)));
    return make_client<Func>(t);
}

template<typename Serializer, typename MsgType>
protocol<Serializer, MsgType>::server::server(protocol<Serializer, MsgType>& proto, ipv4_addr addr) : _proto(proto) {
    listen_options lo;
    lo.reuse_address = true;
    _ss = engine().listen(make_ipv4_address(addr), lo);
    accept();
}

template<typename Serializer, typename MsgType>
void protocol<Serializer, MsgType>::server::accept() {
    keep_doing([this] () mutable {
        return _ss.accept().then([this] (connected_socket fd, socket_address addr) mutable {
            auto conn = make_lw_shared<connection>(*this, std::move(fd), std::move(addr), _proto);
            _conns.insert(conn.get());
            conn->process();
        });
    }).then_wrapped([this] (future<>&& f){
        try {
            f.get();
            assert(false);
        } catch (...) {
            _ss_stopped.set_value();
        }
    });
}

template<typename Serializer, typename MsgType>
protocol<Serializer, MsgType>::server::connection::connection(protocol<Serializer, MsgType>::server& s, connected_socket&& fd, socket_address&& addr, protocol<Serializer, MsgType>& proto)
    : protocol<Serializer, MsgType>::connection(std::move(fd), proto), _server(s) {
    _info.addr = std::move(addr);
}

template <typename MsgType>
future<MsgType, int64_t, temporary_buffer<char>>
read_request_frame(input_stream<char>& in) {
    return in.read_exactly(24).then([&in] (temporary_buffer<char> header) {
        if (header.size() != 24) {
            throw rpc::error("bad response frame header");
        }
        auto ptr = header.get();
        auto type = MsgType(net::ntoh(*unaligned_cast<uint64_t>(ptr)));
        auto msgid = net::ntoh(*unaligned_cast<int64_t*>(ptr + 8));
        auto size = net::ntoh(*unaligned_cast<uint64_t*>(ptr + 16));
        return in.read_exactly(size).then([type, msgid, size] (temporary_buffer<char> data) {
            if (data.size() != size) {
                throw rpc::error("truncated data frame");
            }
            return make_ready_future<MsgType, int64_t, temporary_buffer<char>>(type, msgid, std::move(data));
        });
    });
}

template<typename Serializer, typename MsgType>
future<> protocol<Serializer, MsgType>::server::connection::process() {
    return do_until([this] { return this->_read_buf.eof() || this->_error; }, [this] () mutable {
        return read_request_frame<MsgType>(this->_read_buf).then([this] (MsgType type, int64_t msg_id, temporary_buffer<char> data) {
            //return unmarshall(this->serializer(), this->_read_buf, std::tie(_type)).then([this] {
            auto it = _server._proto._handlers.find(type);
            if (it != _server._proto._handlers.end()) {
                it->second(this->shared_from_this(), msg_id, std::move(data));
            } else {
                this->_error = true;
            }
        });
    }).finally([this, conn_ptr = this->shared_from_this()] () {
        // hold onto connection pointer until do_until() exists
        if (!this->_server._stopping) {
            // if server us stopping to not remove connection
            // since it may invalidate _conns iterators
            this->_server._conns.erase(this);
        }
        this->_stopped.set_value();
    });
}

// FIXME: take out-of-line?
inline
future<int64_t, temporary_buffer<char>>
read_response_frame(input_stream<char>& in) {
    return in.read_exactly(16).then([&in] (temporary_buffer<char> header) {
        if (header.size() != 16) {
            throw rpc::error("bad response frame header");
        }
        auto ptr = header.get();
        auto msgid = net::ntoh(*unaligned_cast<int64_t*>(ptr));
        auto size = net::ntoh(*unaligned_cast<uint64_t*>(ptr + 8));
        return in.read_exactly(size).then([msgid, size] (temporary_buffer<char> data) {
            if (data.size() != size) {
                throw rpc::error("truncated data frame");
            }
            return make_ready_future<int64_t, temporary_buffer<char>>(msgid, std::move(data));
        });
    });
}

template<typename Serializer, typename MsgType>
protocol<Serializer, MsgType>::client::client(protocol<Serializer, MsgType>& proto, ipv4_addr addr, ipv4_addr local) : protocol<Serializer, MsgType>::connection(proto) {
    this->_output_ready = _connected.get_future();
    engine().net().connect(make_ipv4_address(addr), make_ipv4_address(local)).then([this] (connected_socket fd) {
        this->_fd = std::move(fd);
        this->_read_buf = this->_fd.input();
        this->_write_buf = this->_fd.output();
        this->_connected.set_value();
        do_until([this] { return this->_read_buf.eof() || this->_error; }, [this] () mutable {
            return read_response_frame(this->_read_buf).then([this] (int64_t msg_id, temporary_buffer<char> data) {
                //auto unmarshall(this->serializer(), this->_read_buf, std::tie(_rcv_msg_id)).then([this] {
                auto it = _outstanding.find(::abs(msg_id));
                if (it != _outstanding.end()) {
                    auto handler = std::move(it->second);
                    _outstanding.erase(it);
                    (*handler)(*this, msg_id, std::move(data));
                } else {
                    this->_error = true;
                }
            });
        }).finally([this] () {
            this->_error = true;
            this->_write_buf.close();
            _outstanding.clear();
            this->_stopped.set_value();
        });
    });
}

}
