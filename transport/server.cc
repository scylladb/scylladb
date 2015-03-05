/*
 * Copyright 2015 Cloudius Systems
 */

#include "server.hh"

#include "db/consistency_level.hh"
#include "core/future-util.hh"
#include "core/reactor.hh"
#include "utils/UUID.hh"
#include "database.hh"
#include "net/byteorder.hh"

#include "enum_set.hh"
#include "service/query_state.hh"
#include "service/client_state.hh"
#include "transport/protocol_exception.hh"

#include <cassert>
#include <string>

struct cql_frame_error : std::exception {
    const char* what() const throw () override {
        return "bad cql binary frame";
    }
};

struct bad_cql_protocol_version : std::exception {
    const char* what() const throw () override {
        return "bad cql binary protocol version";
    }
};

struct [[gnu::packed]] cql_binary_frame_v1 {
    uint8_t  version;
    uint8_t  flags;
    uint8_t  stream;
    uint8_t  opcode;
    net::packed<uint32_t> length;

    template <typename Adjuster>
    void adjust_endianness(Adjuster a) {
        return a(length);
    }
};

struct [[gnu::packed]] cql_binary_frame_v3 {
    uint8_t  version;
    uint8_t  flags;
    net::packed<uint16_t> stream;
    uint8_t  opcode;
    net::packed<uint32_t> length;

    template <typename Adjuster>
    void adjust_endianness(Adjuster a) {
        return a(stream, length);
    }
};

enum class cql_binary_opcode : uint8_t {
    ERROR          = 0,
    STARTUP        = 1,
    READY          = 2,
    AUTHENTICATE   = 3,
    CREDENTIALS    = 4,
    OPTIONS        = 5,
    SUPPORTED      = 6,
    QUERY          = 7,
    RESULT         = 8,
    PREPARE        = 9,
    EXECUTE        = 10,
    REGISTER       = 11,
    EVENT          = 12,
    BATCH          = 13,
    AUTH_CHALLENGE = 14,
    AUTH_RESPONSE  = 15,
    AUTH_SUCCESS   = 16,
};

enum class cql_binary_error {
    SERVER_ERROR     = 0x0000,
    PROTOCOL_ERROR   = 0x000A,
    BAD_CREDENTIALS  = 0x0100,
    UNAVAILABLE      = 0x1000,
    OVERLOADED       = 0x1001,
    IS_BOOTSTRAPPING = 0x1002,
    TRUNCATE_ERROR   = 0x1003,
    WRITE_TIMEOUT    = 0x1100,
    READ_TIMEOUT     = 0x1200,
    SYNTAX_ERROR     = 0x2000,
    UNAUTHORIZED     = 0x2100,
    INVALID          = 0x2200,
    CONFIG_ERROR     = 0x2300,
    ALREADY_EXISTS   = 0x2400,
    UNPREPARED       = 0x2500,
};

inline db::consistency_level wire_to_consistency(int16_t v)
{
     switch (v) {
     case 0x0000: return db::consistency_level::ANY;
     case 0x0001: return db::consistency_level::ONE;
     case 0x0002: return db::consistency_level::TWO;
     case 0x0003: return db::consistency_level::THREE;
     case 0x0004: return db::consistency_level::QUORUM;
     case 0x0005: return db::consistency_level::ALL;
     case 0x0006: return db::consistency_level::LOCAL_QUORUM;
     case 0x0007: return db::consistency_level::EACH_QUORUM;
     case 0x0008: return db::consistency_level::SERIAL;
     case 0x0009: return db::consistency_level::LOCAL_SERIAL;
     case 0x000A: return db::consistency_level::LOCAL_ONE;
     default: assert(0);
     }
}

inline int16_t consistency_to_wire(db::consistency_level c)
{
    switch (c) {
    case db::consistency_level::ANY:          return 0x0000;
    case db::consistency_level::ONE:          return 0x0001;
    case db::consistency_level::TWO:          return 0x0002;
    case db::consistency_level::THREE:        return 0x0003;
    case db::consistency_level::QUORUM:       return 0x0004;
    case db::consistency_level::ALL:          return 0x0005;
    case db::consistency_level::LOCAL_QUORUM: return 0x0006;
    case db::consistency_level::EACH_QUORUM:  return 0x0007;
    case db::consistency_level::SERIAL:       return 0x0008;
    case db::consistency_level::LOCAL_SERIAL: return 0x0009;
    case db::consistency_level::LOCAL_ONE:    return 0x000A;
    default: assert(0);
    }
}

struct cql_query_state {
    service::query_state query_state;
    std::unique_ptr<cql3::query_options> options;

    cql_query_state(service::client_state& client_state)
        : query_state(client_state)
    { }
};

class cql_server::connection {
    cql_server& _server;
    connected_socket _fd;
    input_stream<char> _read_buf;
    output_stream<char> _write_buf;
    uint8_t _version = 0;
    service::client_state _client_state;
    std::unordered_map<uint16_t, cql_query_state> _query_states;
public:
    connection(cql_server& server, connected_socket&& fd, socket_address addr)
        : _server(server)
        , _fd(std::move(fd))
        , _read_buf(_fd.input())
        , _write_buf(_fd.output())
        , _client_state(service::client_state::for_external_calls())
    { }
    future<> process() {
        return do_until([this] { return _read_buf.eof(); }, [this] { return process_request(); });
    }
    future<> process_request();
private:
    unsigned frame_size() const;
    cql_binary_frame_v3 parse_frame(temporary_buffer<char> buf);
    future<std::experimental::optional<cql_binary_frame_v3>> read_frame();
    future<> process_startup(uint16_t stream, temporary_buffer<char> buf);
    future<> process_auth_response(uint16_t stream, temporary_buffer<char> buf);
    future<> process_options(uint16_t stream, temporary_buffer<char> buf);
    future<> process_query(uint16_t stream, temporary_buffer<char> buf);
    future<> process_prepare(uint16_t stream, temporary_buffer<char> buf);
    future<> process_execute(uint16_t stream, temporary_buffer<char> buf);
    future<> process_batch(uint16_t stream, temporary_buffer<char> buf);
    future<> process_register(uint16_t stream, temporary_buffer<char> buf);

    future<> write_error(int16_t stream, cql_binary_error err, sstring msg);
    future<> write_ready(int16_t stream);
    future<> write_supported(int16_t stream);
    future<> write_result(int16_t stream, shared_ptr<transport::messages::result_message> msg);
    future<> write_response(shared_ptr<cql_server::response> response);

    void check_room(temporary_buffer<char>& buf, size_t n) {
        if (buf.size() < n) {
            throw transport::protocol_exception("truncated frame");
        }
    }

    int8_t read_byte(temporary_buffer<char>& buf);
    int32_t read_int(temporary_buffer<char>& buf);
    int64_t read_long(temporary_buffer<char>& buf);
    int16_t read_short(temporary_buffer<char>& buf);
    uint16_t read_unsigned_short(temporary_buffer<char>& buf);
    sstring read_string(temporary_buffer<char>& buf);
    bytes_opt read_value(temporary_buffer<char>& buf);
    sstring_view read_long_string_view(temporary_buffer<char>& buf);
    void read_name_and_value_list(temporary_buffer<char>& buf, std::vector<sstring>& names, std::vector<bytes_opt>& values);
    void read_value_list(temporary_buffer<char>& buf, std::vector<bytes_opt>& values);
    db::consistency_level read_consistency(temporary_buffer<char>& buf);
    std::unordered_map<sstring, sstring> read_string_map(temporary_buffer<char>& buf);
    std::unique_ptr<cql3::query_options> read_options(temporary_buffer<char>& buf);

    cql_query_state& get_query_state(uint16_t stream);
};

class cql_server::response {
    int16_t           _stream;
    cql_binary_opcode _opcode;
    std::stringstream _body;
public:
    response(int16_t stream, cql_binary_opcode opcode)
        : _stream{stream}
        , _opcode{opcode}
    { }
    scattered_message<char> make_message(uint8_t version);
    void write_int(int32_t n);
    void write_long(int64_t n);
    void write_short(int16_t n);
    void write_string(const sstring& s);
    void write_long_string(const sstring& s);
    void write_uuid(utils::UUID uuid);
    void write_string_list(std::vector<sstring> string_list);
    void write_bytes(bytes b);
    void write_short_bytes(bytes b);
    void write_option(std::pair<int16_t, boost::any> opt);
    void write_option_list(std::vector<std::pair<int16_t, boost::any>> opt_list);
    void write_inet(ipv4_addr inet);
    void write_consistency(db::consistency_level c);
    void write_string_map(std::map<sstring, sstring> string_map);
    void write_string_multimap(std::multimap<sstring, sstring> string_map);
private:
    sstring make_frame(uint8_t version, size_t length);
};

cql_server::cql_server(distributed<database>& db)
    : _proxy(db)
    , _query_processor(_proxy, db)
{
}

future<>
cql_server::listen(ipv4_addr addr) {
    listen_options lo;
    lo.reuse_address = true;
    _listeners.push_back(engine().listen(make_ipv4_address(addr), lo));
    do_accepts(_listeners.size() - 1);
    return make_ready_future<>();
}

void
cql_server::do_accepts(int which) {
    _listeners[which].accept().then([this, which] (connected_socket fd, socket_address addr) mutable {
        auto conn = make_shared<connection>(*this, std::move(fd), addr);
        conn->process().then_wrapped([this, conn] (future<> f) {
            try {
                f.get();
            } catch (std::exception& ex) {
                std::cout << "request error " << ex.what() << "\n";
            }
        });
        do_accepts(which);
    }).then_wrapped([] (future<> f) {
        try {
            f.get();
        } catch (std::exception& ex) {
            std::cout << "accept failed: " << ex.what() << "\n";
        }
    });
}

unsigned
cql_server::connection::frame_size() const {
    if (_version < 3) {
        return 8;
    } else {
        return 9;
    }
}

cql_binary_frame_v3
cql_server::connection::parse_frame(temporary_buffer<char> buf) {
    if (buf.size() != frame_size()) {
        throw cql_frame_error();
    }
    cql_binary_frame_v3 v3;
    switch (_version) {
    case 1:
    case 2: {
        auto raw = reinterpret_cast<const cql_binary_frame_v1*>(buf.get());
        auto cooked = net::ntoh(*raw);
        v3.version = cooked.version;
        v3.flags = cooked.flags;
        v3.opcode = cooked.opcode;
        v3.stream = cooked.stream;
        v3.length = cooked.length;
        break;
    }
    case 3:
    case 4: {
        v3 = net::ntoh(*reinterpret_cast<const cql_binary_frame_v3*>(buf.get()));
        break;
    }
    default:
        abort();
    }
    if (v3.version != _version) {
        throw bad_cql_protocol_version();
    }
    return v3;
}

future<std::experimental::optional<cql_binary_frame_v3>>
cql_server::connection::read_frame() {
    using ret_type = std::experimental::optional<cql_binary_frame_v3>;
    if (!_version) {
        // We don't know the frame size before reading the first frame,
        // so read just one byte, and then read the rest of the frame.
        return _read_buf.read_exactly(1).then([this] (temporary_buffer<char> buf) {
            if (buf.empty()) {
                return make_ready_future<ret_type>();
            }
            _version = buf[0];
            if (_version < 1 || _version > 4) {
                throw bad_cql_protocol_version();
            }
            return _read_buf.read_exactly(frame_size() - 1).then([this] (temporary_buffer<char> tail) {
                temporary_buffer<char> full(frame_size());
                full.get_write()[0] = _version;
                std::copy(tail.get(), tail.get() + tail.size(), full.get_write() + 1);
                return make_ready_future<ret_type>(parse_frame(std::move(full)));
            });
        });
    } else {
        // Not the first frame, so we know the size.
        return _read_buf.read_exactly(frame_size()).then([this] (temporary_buffer<char> buf) {
            if (buf.empty()) {
                return make_ready_future<ret_type>();
            }
            return make_ready_future<ret_type>(parse_frame(std::move(buf)));
        });
    }
}

future<> cql_server::connection::process_request() {
    return read_frame().then_wrapped([this] (future<std::experimental::optional<cql_binary_frame_v3>>&& v) {
        auto maybe_frame = std::get<0>(v.get());
        if (!maybe_frame) {
            // eof
            return make_ready_future<>();
        }
        auto& f = *maybe_frame;
        return _read_buf.read_exactly(f.length).then([this, f] (temporary_buffer<char> buf) {
            assert(!(f.flags & 0x01)); // FIXME: compression
            switch (static_cast<cql_binary_opcode>(f.opcode)) {
            case cql_binary_opcode::STARTUP:       return process_startup(f.stream, std::move(buf));
            case cql_binary_opcode::AUTH_RESPONSE: return process_auth_response(f.stream, std::move(buf));
            case cql_binary_opcode::OPTIONS:       return process_options(f.stream, std::move(buf));
            case cql_binary_opcode::QUERY:         return process_query(f.stream, std::move(buf));
            case cql_binary_opcode::PREPARE:       return process_prepare(f.stream, std::move(buf));
            case cql_binary_opcode::EXECUTE:       return process_execute(f.stream, std::move(buf));
            case cql_binary_opcode::BATCH:         return process_batch(f.stream, std::move(buf));
            case cql_binary_opcode::REGISTER:      return process_register(f.stream, std::move(buf));
            default: assert(0);
            };
        });
    }).then_wrapped([] (future<> f) {
        try {
            f.get();
        } catch (std::exception& ex) {
            std::cout << "request processing failed: " << ex.what() << "\n";
        }
    });
}

future<> cql_server::connection::process_startup(uint16_t stream, temporary_buffer<char> buf)
{
    auto string_map = read_string_map(buf);
    for (auto&& s : string_map) {
        print("%s => %s\n", s.first, s.second);
    }
    return write_ready(stream);
}

future<> cql_server::connection::process_auth_response(uint16_t stream, temporary_buffer<char> buf)
{
    assert(0);
    return make_ready_future<>();
}

future<> cql_server::connection::process_options(uint16_t stream, temporary_buffer<char> buf)
{
    return write_supported(stream);
}

cql_query_state& cql_server::connection::get_query_state(uint16_t stream)
{
    auto i = _query_states.find(stream);
    if (i == _query_states.end()) {
        i = _query_states.emplace(stream, _client_state).first;
    }
    return i->second;
}

future<> cql_server::connection::process_query(uint16_t stream, temporary_buffer<char> buf)
{
    auto query = read_long_string_view(buf);
#if 0
    auto consistency = read_consistency(buf);
    auto flags = read_byte(buf);
#endif
    auto& q_state = get_query_state(stream);
    q_state.options = read_options(buf);
    return _server._query_processor.process(query, q_state.query_state, *q_state.options).then([this, stream] (auto msg) {
         return this->write_result(stream, msg);
    });
}

future<> cql_server::connection::process_prepare(uint16_t stream, temporary_buffer<char> buf)
{
    assert(0);
    return make_ready_future<>();
}

future<> cql_server::connection::process_execute(uint16_t stream, temporary_buffer<char> buf)
{
    assert(0);
    return make_ready_future<>();
}

future<> cql_server::connection::process_batch(uint16_t stream, temporary_buffer<char> buf)
{
    assert(0);
    return make_ready_future<>();
}

future<> cql_server::connection::process_register(uint16_t stream, temporary_buffer<char> buf)
{
    print("warning: ignoring event registration\n");
    return write_ready(stream);
}

future<> cql_server::connection::write_error(int16_t stream, cql_binary_error err, sstring msg)
{
    auto response = make_shared<cql_server::response>(stream, cql_binary_opcode::ERROR);
    response->write_int(static_cast<int32_t>(err));
    response->write_string(msg);
    return write_response(response);
}

future<> cql_server::connection::write_ready(int16_t stream)
{
    auto response = make_shared<cql_server::response>(stream, cql_binary_opcode::READY);
    return write_response(response);
}

future<> cql_server::connection::write_supported(int16_t stream)
{
    std::multimap<sstring, sstring> opts;
    opts.insert({"CQL_VERSION", "3.0.0"});
    opts.insert({"CQL_VERSION", "3.2.0"});
    opts.insert({"COMPRESSION", "snappy"});
    auto response = make_shared<cql_server::response>(stream, cql_binary_opcode::SUPPORTED);
    response->write_string_multimap(opts);
    return write_response(response);
}

class cql_server::fmt_visitor : public transport::messages::result_message::visitor {
private:
    shared_ptr<cql_server::response> _response;
public:
    fmt_visitor(shared_ptr<cql_server::response> response)
        : _response{response}
    { }

    virtual void visit(const transport::messages::result_message::void_message&) override {
        _response->write_int(0x0001);
    }

    virtual void visit(const transport::messages::result_message::set_keyspace& m) override {
        _response->write_int(0x0003);
        _response->write_string(m.get_keyspace());
    }
};

future<> cql_server::connection::write_result(int16_t stream, shared_ptr<transport::messages::result_message> msg)
{
    auto response = make_shared<cql_server::response>(stream, cql_binary_opcode::RESULT);
    fmt_visitor fmt{response};
    msg->accept(fmt);
    return write_response(response);
}

future<> cql_server::connection::write_response(shared_ptr<cql_server::response> response)
{
    auto msg = response->make_message(_version);
    return _write_buf.write(std::move(msg)).then([this] {
        return _write_buf.flush();
    });
}

int8_t cql_server::connection::read_byte(temporary_buffer<char>& buf)
{
    check_room(buf, 1);
    int8_t n = buf[0];
    buf.trim_front(1);
    return n;
}

int32_t cql_server::connection::read_int(temporary_buffer<char>& buf)
{
    check_room(buf, sizeof(int32_t));
    auto p = reinterpret_cast<const uint8_t*>(buf.begin());
    uint32_t n = (static_cast<uint32_t>(p[0]) << 24)
               | (static_cast<uint32_t>(p[1]) << 16)
               | (static_cast<uint32_t>(p[2]) << 8)
               | (static_cast<uint32_t>(p[3]));
    buf.trim_front(4);
    return n;
}

int64_t cql_server::connection::read_long(temporary_buffer<char>& buf)
{
    check_room(buf, sizeof(int64_t));
    auto p = reinterpret_cast<const uint8_t*>(buf.begin());
    uint64_t n = (static_cast<uint64_t>(p[0]) << 56)
               | (static_cast<uint64_t>(p[1]) << 48)
               | (static_cast<uint64_t>(p[2]) << 40)
               | (static_cast<uint64_t>(p[3]) << 32)
               | (static_cast<uint64_t>(p[4]) << 24)
               | (static_cast<uint64_t>(p[5]) << 16)
               | (static_cast<uint64_t>(p[6]) << 8)
               | (static_cast<uint64_t>(p[7]));
    buf.trim_front(8);
    return n;
}

int16_t cql_server::connection::read_short(temporary_buffer<char>& buf)
{
    return static_cast<int16_t>(read_unsigned_short(buf));
}

uint16_t cql_server::connection::read_unsigned_short(temporary_buffer<char>& buf)
{
    check_room(buf, sizeof(uint16_t));
    auto p = reinterpret_cast<const uint8_t*>(buf.begin());
    uint16_t n = (static_cast<uint16_t>(p[0]) << 8)
               | (static_cast<uint16_t>(p[1]));
    buf.trim_front(2);
    return n;
}

sstring cql_server::connection::read_string(temporary_buffer<char>& buf)
{
    auto n = read_short(buf);
    check_room(buf, n);
    sstring s{buf.begin(), static_cast<size_t>(n)};
    assert(n >= 0);
    buf.trim_front(n);
    return s;
}

sstring_view cql_server::connection::read_long_string_view(temporary_buffer<char>& buf)
{
    auto n = read_int(buf);
    check_room(buf, n);
    sstring_view s{buf.begin(), static_cast<size_t>(n)};
    buf.trim_front(n);
    return s;
}

db::consistency_level cql_server::connection::read_consistency(temporary_buffer<char>& buf)
{
    return wire_to_consistency(read_short(buf));
}

std::unordered_map<sstring, sstring> cql_server::connection::read_string_map(temporary_buffer<char>& buf)
{
    std::unordered_map<sstring, sstring> string_map;
    auto n = read_short(buf);
    for (auto i = 0; i < n; i++) {
        auto key = read_string(buf);
        auto val = read_string(buf);
        string_map.emplace(std::piecewise_construct,
            std::forward_as_tuple(std::move(key)),
            std::forward_as_tuple(std::move(val)));
    }
    return string_map;
}

enum class options_flag {
    VALUES,
    SKIP_METADATA,
    PAGE_SIZE,
    PAGING_STATE,
    SERIAL_CONSISTENCY,
    TIMESTAMP,
    NAMES_FOR_VALUES
};

using options_flag_enum = super_enum<options_flag,
    options_flag::VALUES,
    options_flag::SKIP_METADATA,
    options_flag::PAGE_SIZE,
    options_flag::PAGING_STATE,
    options_flag::SERIAL_CONSISTENCY,
    options_flag::TIMESTAMP,
    options_flag::NAMES_FOR_VALUES
>;

std::unique_ptr<cql3::query_options> cql_server::connection::read_options(temporary_buffer<char>& buf)
{
    auto consistency = read_consistency(buf);
    if (_version == 1) {
        return std::make_unique<cql3::default_query_options>(consistency, std::vector<bytes_opt>{},
            false, cql3::query_options::specific_options::DEFAULT, 1);
    }

    assert(_version >= 2);

    auto flags = enum_set<options_flag_enum>::from_mask(read_byte(buf));
    std::vector<bytes_opt> values;
    std::vector<sstring> names;

    if (flags.contains<options_flag::VALUES>()) {
        if (flags.contains<options_flag::NAMES_FOR_VALUES>()) {
            read_name_and_value_list(buf, names, values);
        } else {
            read_value_list(buf, values);
        }
    }

    bool skip_metadata = flags.contains<options_flag::SKIP_METADATA>();
    flags.remove<options_flag::VALUES>();
    flags.remove<options_flag::SKIP_METADATA>();

    std::unique_ptr<cql3::query_options> options;
    if (flags) {
        ::shared_ptr<service::pager::paging_state> paging_state;
        int32_t page_size = flags.contains<options_flag::PAGE_SIZE>() ? read_int(buf) : -1;
        if (flags.contains<options_flag::PAGING_STATE>()) {
            fail(unimplemented::cause::PAGING);
#if 0
            paging_state = PagingState.deserialize(CBUtil.readValue(body))
#endif
        }

        db::consistency_level serial_consistency = db::consistency_level::SERIAL;
        if (flags.contains<options_flag::SERIAL_CONSISTENCY>()) {
            serial_consistency = read_consistency(buf);
        }

        api::timestamp_type ts = api::missing_timestamp;
        if (flags.contains<options_flag::TIMESTAMP>()) {
            ts = read_long(buf);
            if (ts < api::min_timestamp || ts > api::max_timestamp) {
                throw transport::protocol_exception(sprint("Out of bound timestamp, must be in [%d, %d] (got %d)",
                    api::min_timestamp, api::max_timestamp, ts));
            }
        }

        options = std::make_unique<cql3::default_query_options>(consistency, std::move(values), skip_metadata,
            cql3::query_options::specific_options{page_size, std::move(paging_state), serial_consistency, ts}, _version);
    } else {
        options = std::make_unique<cql3::default_query_options>(consistency, std::move(values), skip_metadata,
            cql3::query_options::specific_options::DEFAULT, _version);
    }

    if (names.empty()) {
        return std::move(options);
    }

    return std::make_unique<cql3::options_with_names>(std::move(options), std::move(names));
}

void cql_server::connection::read_name_and_value_list(temporary_buffer<char>& buf, std::vector<sstring>& names, std::vector<bytes_opt>& values) {
    uint16_t size = read_unsigned_short(buf);
    names.reserve(size);
    values.reserve(size);
    for (uint16_t i = 0; i < size; i++) {
        names.emplace_back(read_string(buf));
        values.emplace_back(read_value(buf));
    }
}

void cql_server::connection::read_value_list(temporary_buffer<char>& buf, std::vector<bytes_opt>& values) {
    uint16_t size = read_unsigned_short(buf);
    values.reserve(size);
    for (uint16_t i = 0; i < size; i++) {
        values.emplace_back(read_value(buf));
    }
}

bytes_opt cql_server::connection::read_value(temporary_buffer<char>& buf) {
    auto len = read_int(buf);
    if (len < 0) {
        return {};
    }
    check_room(buf, len);
    bytes b(buf.begin(), buf.begin() + len);
    buf.trim_front(len);
    return {std::move(b)};
}

scattered_message<char> cql_server::response::make_message(uint8_t version) {
    scattered_message<char> msg;
    sstring body = _body.str();
    sstring frame = make_frame(version, body.size());
    msg.append(std::move(frame));
    msg.append(std::move(body));
    return msg;
}

sstring cql_server::response::make_frame(uint8_t version, size_t length)
{
    switch (version) {
    case 0x01:
    case 0x02: {
        sstring frame_buf(sstring::initialized_later(), sizeof(cql_binary_frame_v1));
        auto* frame = reinterpret_cast<cql_binary_frame_v1*>(frame_buf.begin());
        frame->version = version | 0x80;
        frame->flags   = 0x00;
        frame->stream  = _stream;
        frame->opcode  = static_cast<uint8_t>(_opcode);
        frame->length  = htonl(length);
        return frame_buf;
    }
    case 0x03:
    case 0x04: {
        sstring frame_buf(sstring::initialized_later(), sizeof(cql_binary_frame_v3));
        auto* frame = reinterpret_cast<cql_binary_frame_v3*>(frame_buf.begin());
        frame->version = version | 0x80;
        frame->flags   = 0x00;
        frame->stream  = htons(_stream);
        frame->opcode  = static_cast<uint8_t>(_opcode);
        frame->length  = htonl(length);
        return frame_buf;
    }
    default:
        assert(0);
    }
}

void cql_server::response::write_int(int32_t n)
{
    auto u = htonl(n);
    _body.write(reinterpret_cast<const char*>(&u), sizeof(u));
}

void cql_server::response::write_long(int64_t n)
{
    auto u = htonq(n);
    _body.write(reinterpret_cast<const char*>(&u), sizeof(u));
}

void cql_server::response::write_short(int16_t n)
{
    auto u = htons(n);
    _body.write(reinterpret_cast<const char*>(&u), sizeof(u));
}

void cql_server::response::write_string(const sstring& s)
{
    assert(s.size() < std::numeric_limits<int16_t>::max());
    write_short(s.size());
    _body << s;
}

void cql_server::response::write_long_string(const sstring& s)
{
    assert(s.size() < std::numeric_limits<int32_t>::max());
    write_int(s.size());
    _body << s;
}

void cql_server::response::write_uuid(utils::UUID uuid)
{
    // FIXME
    assert(0);
}

void cql_server::response::write_string_list(std::vector<sstring> string_list)
{
    assert(string_list.size() < std::numeric_limits<int16_t>::max());
    write_short(string_list.size());
    for (auto&& s : string_list) {
        write_string(s);
    }
}

void cql_server::response::write_bytes(bytes b)
{
    assert(b.size() < std::numeric_limits<int32_t>::max());
    write_int(b.size());
    _body << b;
}

void cql_server::response::write_short_bytes(bytes b)
{
    assert(b.size() < std::numeric_limits<int16_t>::max());
    write_short(b.size());
    _body << b;
}

void cql_server::response::write_option(std::pair<int16_t, boost::any> opt)
{
    // FIXME
    assert(0);
}

void cql_server::response::write_option_list(std::vector<std::pair<int16_t, boost::any>> opt_list)
{
    // FIXME
    assert(0);
}

void cql_server::response::write_inet(ipv4_addr inet)
{
    // FIXME
    assert(0);
}

void cql_server::response::write_consistency(db::consistency_level c)
{
    write_short(consistency_to_wire(c));
}

void cql_server::response::write_string_map(std::map<sstring, sstring> string_map)
{
    assert(string_map.size() < std::numeric_limits<int16_t>::max());
    write_short(string_map.size());
    for (auto&& s : string_map) {
        write_string(s.first);
        write_string(s.second);
    }
}

void cql_server::response::write_string_multimap(std::multimap<sstring, sstring> string_map)
{
    std::vector<sstring> keys;
    for (auto it = string_map.begin(), end = string_map.end(); it != end; it = string_map.upper_bound(it->first)) {
        keys.push_back(it->first);
    }
    assert(keys.size() < std::numeric_limits<int16_t>::max());
    write_short(keys.size());
    for (auto&& key : keys) {
        std::vector<sstring> values;
        auto range = string_map.equal_range(key);
        for (auto it = range.first; it != range.second; ++it) {
            values.push_back(it->second);
        }
        write_string(key);
        write_string_list(values);
    }
}
