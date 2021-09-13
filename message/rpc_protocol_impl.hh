#include "message/messaging_service.hh"

#include <seastar/rpc/rpc.hh>

namespace netw {

// thunk from rpc serializers to generate serializers
template <typename T, typename Output>
void write(serializer, Output& out, const T& data) {
    ser::serialize(out, data);
}
template <typename T, typename Input>
T read(serializer, Input& in, boost::type<T> type) {
    return ser::deserialize(in, type);
}

template <typename Output, typename T>
void write(serializer s, Output& out, const foreign_ptr<T>& v) {
    return write(s, out, *v);
}
template <typename Input, typename T>
foreign_ptr<T> read(serializer s, Input& in, boost::type<foreign_ptr<T>>) {
    return make_foreign(read(s, in, boost::type<T>()));
}

template <typename Output, typename T>
void write(serializer s, Output& out, const lw_shared_ptr<T>& v) {
    return write(s, out, *v);
}
template <typename Input, typename T>
lw_shared_ptr<T> read(serializer s, Input& in, boost::type<lw_shared_ptr<T>>) {
    return make_lw_shared<T>(read(s, in, boost::type<T>()));
}

using rpc_protocol = rpc::protocol<serializer, messaging_verb>;

class messaging_service::rpc_protocol_wrapper {
    rpc_protocol _impl;
public:
    explicit rpc_protocol_wrapper(serializer &&s) : _impl(std::move(s)) {}

    rpc_protocol &protocol() { return _impl; }

    template<typename Func>
    auto make_client(messaging_verb t) { return _impl.make_client<Func>(t); }

    template<typename Func>
    auto register_handler(messaging_verb t, Func &&func) {
        return _impl.register_handler(t, std::forward<Func>(func));
    }

    template<typename Func>
    auto register_handler(messaging_verb t, scheduling_group sg, Func &&func) {
        return _impl.register_handler(t, sg, std::forward<Func>(func));
    }

    future<> unregister_handler(messaging_verb t) { return _impl.unregister_handler(t); }

    void set_logger(::seastar::logger *logger) { _impl.set_logger(logger); }

    bool has_handler(messaging_verb msg_id) { return _impl.has_handler(msg_id); }

    bool has_handlers() const noexcept { return _impl.has_handlers(); }
};

// This wrapper pretends to be rpc_protocol::client, but also handles
// stopping it before destruction, in case it wasn't stopped already.
// This should be integrated into messaging_service proper.
class messaging_service::rpc_protocol_client_wrapper {
    std::unique_ptr<rpc_protocol::client> _p;
    ::shared_ptr<seastar::tls::server_credentials> _credentials;
public:
    rpc_protocol_client_wrapper(rpc_protocol &proto, rpc::client_options opts, socket_address addr,
                                socket_address local = {})
            : _p(std::make_unique<rpc_protocol::client>(proto, std::move(opts), addr, local)) {
    }

    rpc_protocol_client_wrapper(rpc_protocol &proto, rpc::client_options opts, socket_address addr,
                                socket_address local, ::shared_ptr<seastar::tls::server_credentials> c)
            : _p(
            std::make_unique<rpc_protocol::client>(proto, std::move(opts), seastar::tls::socket(c), addr, local)),
              _credentials(c) {}

    auto get_stats() const { return _p->get_stats(); }

    future<> stop() { return _p->stop(); }

    bool error() {
        return _p->error();
    }

    operator rpc_protocol::client &() { return *_p; }

    /**
     * #3787 Must ensure we use the right type of socket. I.e. tls or not.
     * See above, we retain credentials object so we here can know if we
     * are tls or not.
     */
    template<typename Serializer, typename... Out>
    future<rpc::sink<Out...>> make_stream_sink() {
        if (_credentials) {
            return _p->make_stream_sink<Serializer, Out...>(seastar::tls::socket(_credentials));
        }
        return _p->make_stream_sink<Serializer, Out...>();
    }
};

// Register a handler (a callback lambda) for verb
template<typename Func>
void register_handler(messaging_service *ms, messaging_verb verb, Func &&func) {
    ms->rpc()->register_handler(verb, ms->scheduling_group_for_verb(verb), std::move(func));
}

// Send a message for verb
template <typename MsgIn, typename... MsgOut>
auto send_message(messaging_service* ms, messaging_verb verb, msg_addr id, MsgOut&&... msg) {
    auto rpc_handler = ms->rpc()->make_client<MsgIn(MsgOut...)>(verb);
    if (ms->is_shutting_down()) {
        using futurator = futurize<std::result_of_t<decltype(rpc_handler)(rpc_protocol::client&, MsgOut...)>>;
        return futurator::make_exception_future(rpc::closed_error());
    }
    auto rpc_client_ptr = ms->get_rpc_client(verb, id);
    auto& rpc_client = *rpc_client_ptr;
    return rpc_handler(rpc_client, std::forward<MsgOut>(msg)...).then_wrapped([ms = ms->shared_from_this(), id, verb, rpc_client_ptr = std::move(rpc_client_ptr)] (auto&& f) {
        try {
            if (f.failed()) {
                ms->increment_dropped_messages(verb);
                f.get();
                assert(false); // never reached
            }
            return std::move(f);
        } catch (rpc::closed_error&) {
            // This is a transport error
            ms->remove_error_rpc_client(verb, id);
            throw;
        } catch (...) {
            // This is expected to be a rpc server error, e.g., the rpc handler throws a std::runtime_error.
            throw;
        }
    });
}

// TODO: Remove duplicated code in send_message
template <typename MsgIn, typename Timeout, typename... MsgOut>
auto send_message_timeout(messaging_service* ms, messaging_verb verb, msg_addr id, Timeout timeout, MsgOut&&... msg) {
    auto rpc_handler = ms->rpc()->make_client<MsgIn(MsgOut...)>(verb);
    if (ms->is_shutting_down()) {
        using futurator = futurize<std::result_of_t<decltype(rpc_handler)(rpc_protocol::client&, MsgOut...)>>;
        return futurator::make_exception_future(rpc::closed_error());
    }
    auto rpc_client_ptr = ms->get_rpc_client(verb, id);
    auto& rpc_client = *rpc_client_ptr;
    return rpc_handler(rpc_client, timeout, std::forward<MsgOut>(msg)...).then_wrapped([ms = ms->shared_from_this(), id, verb, rpc_client_ptr = std::move(rpc_client_ptr)] (auto&& f) {
        try {
            if (f.failed()) {
                ms->increment_dropped_messages(verb);
                f.get();
                assert(false); // never reached
            }
            return std::move(f);
        } catch (rpc::closed_error&) {
            // This is a transport error
            ms->remove_error_rpc_client(verb, id);
            throw;
        } catch (...) {
            // This is expected to be a rpc server error, e.g., the rpc handler throws a std::runtime_error.
            throw;
        }
    });
}

// Send one way message for verb
template <typename... MsgOut>
auto send_message_oneway(messaging_service* ms, messaging_verb verb, msg_addr id, MsgOut&&... msg) {
    return send_message<rpc::no_wait_type>(ms, std::move(verb), std::move(id), std::forward<MsgOut>(msg)...);
}

// Send one way message for verb
template <typename Timeout, typename... MsgOut>
auto send_message_oneway_timeout(messaging_service* ms, Timeout timeout, messaging_verb verb, msg_addr id, MsgOut&&... msg) {
    return send_message_timeout<rpc::no_wait_type>(ms, std::move(verb), std::move(id), timeout, std::forward<MsgOut>(msg)...);
}

} // namespace netw