/*
 * Copyright (C) 2019-present ScyllaDB
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

#include "lua.hh"
#include "exceptions/exceptions.hh"
#include "concrete_types.hh"
#include "utils/utf8.hh"
#include "utils/ascii.hh"
#include "utils/date.h"
#include <seastar/core/align.hh>
#include <lua.hpp>
#include "db/config.hh"

// Lua 5.4 added an extra parameter to lua_resume

#if LUA_VERSION_NUM >= 504
#    define LUA_504_PLUS(x...) x
#else
#    define LUA_504_PLUS(x...)
#endif

using namespace seastar;

static logging::logger lua_logger("lua");

namespace {
struct alloc_state {
    size_t allocated = 0;
    size_t max;
    size_t max_contiguous;
    alloc_state(size_t max, size_t max_contiguous)
        : max(max)
        , max_contiguous(max_contiguous) {
        // The max and max_contiguous limits are responsible for avoiding overflows.
        assert(max + max_contiguous >= max);
    }
};

struct lua_closer {
    void operator()(lua_State* l) {
        lua_close(l);
    }
};

static const char scylla_decimal_metatable_name[] = "Scylla.decimal";

class lua_slice_state {
    std::unique_ptr<alloc_state> a_state;
    std::unique_ptr<lua_State, lua_closer> _l;
public:
    lua_slice_state(std::unique_ptr<alloc_state> a_state, std::unique_ptr<lua_State, lua_closer> l)
        : a_state(std::move(a_state))
        , _l(std::move(l)) {}
    operator lua_State*() { return _l.get(); }
};
}

static void* lua_alloc(void* ud, void* ptr, size_t osize, size_t nsize) {
    auto* s = reinterpret_cast<alloc_state*>(ud);

    // avoid realloc(nullptr, 0), which would allocate.
    if (nsize == 0 && ptr == nullptr) {
        return nullptr;
    }

    if (nsize > s->max_contiguous) {
        return nullptr;
    }

    size_t next = s->allocated + nsize;

    // The max and max_contiguous limits should be small enough to avoid overflows.
    assert(next >= s->allocated);

    if (ptr) {
        next -= osize;
    }

    if (next > s->max) {
        lua_logger.info("allocation failed. alread allocated = {}, next total = {}, max = {}", s->allocated, next, s->max);
        return nullptr;
    }

    // FIXME: Given that we have osize, we can probably do better when
    // SEASTAR_DEFAULT_ALLOCATOR is false
    void* ret = realloc(ptr, nsize);

    if (nsize == 0 || ret != nullptr) {
        s->allocated = next;
    }
    return ret;
}

static const luaL_Reg loadedlibs[] = {
    {"_G", luaopen_base},
    {LUA_COLIBNAME, luaopen_string},
    {LUA_COLIBNAME, luaopen_coroutine},
    {LUA_TABLIBNAME, luaopen_table},
    {NULL, NULL},
};

static void debug_hook(lua_State* l, lua_Debug* ar) {
    if (!need_preempt()) {
        return;
    }
    // The lua manual says that only count and line events can yield. Of those we only use count.
    if (ar->event != LUA_HOOKCOUNT) {
        // Set the hook to stop at the very next lua instruction, where we will be able to yield.
        lua_sethook(l, debug_hook, LUA_MASKCOUNT, 1);
        return;
    }
    if (lua_yield(l, 0)) {
        assert(0 && "lua_yield failed");
    }
}

static lua_slice_state new_lua(const lua::runtime_config& cfg) {
    auto a_state = std::make_unique<alloc_state>(cfg.max_bytes, cfg.max_contiguous);
    std::unique_ptr<lua_State, lua_closer> l{lua_newstate(lua_alloc, a_state.get())};
    if (!l) {
        throw std::runtime_error("could not create lua state");
    }
    return lua_slice_state{std::move(a_state), std::move(l)};
}

static int string_writer(lua_State*, const void* p, size_t size, void* data) {
    luaL_addlstring(reinterpret_cast<luaL_Buffer*>(data), reinterpret_cast<const char*>(p), size);
    return 0;
}

static int compile_l(lua_State* l) {
    const auto& script = *reinterpret_cast<sstring*>(lua_touserdata(l, 1));
    luaL_Buffer buf;
    luaL_buffinit(l, &buf);

    if (luaL_loadbufferx(l, script.c_str(), script.size(), "<internal>", "t")) {
        lua_error(l);
    }
    if (lua_dump(l, string_writer, &buf, true)) {
        luaL_error(l, "lua_dump failed");
    }
    luaL_pushresult(&buf);

    return 1;
}

sstring lua::compile(const runtime_config& cfg, const std::vector<sstring>& arg_names, sstring script) {
    if (!arg_names.empty()) {
        // In Lua, all chunks are compiled to vararg functions. To use
        // the UDF argument names, we start the chunk with "local
        // arg1,arg2,etc = ...", which captures the arguments when the
        // chunk is called.
        std::ostringstream os;
        os << "local ";
        for (int i = 0, n = arg_names.size(); i < n; ++i) {
            if (i != 0) {
                os << ",";
            }
            os << arg_names[i];
        }
        os << " = ...;\n" << script;
        script = os.str();
    }
    lua_slice_state l = new_lua(cfg);

    // Run the load from lua_pcall so we don't have to handle longjmp.
    lua_pushcfunction(l, compile_l);
    lua_pushlightuserdata(l, &script);
    if (lua_pcall(l, 1, 1, 0)) {
        throw exceptions::invalid_request_exception(std::string("could not compile: ") + lua_tostring(l, -1));
    }

    size_t len;
    const char* p = lua_tolstring(l, -1, &len);
    return sstring(p, len);
}

static big_decimal* get_decimal(lua_State* l, int arg) {
    constexpr size_t alignment = alignof(big_decimal);
    char* p = reinterpret_cast<char*>(luaL_testudata(l, arg, scylla_decimal_metatable_name));
    if (p) {
        return reinterpret_cast<big_decimal*>(align_up(p, alignment));
    }
    return nullptr;
}

template <typename T>
static T* aligned_used_data(lua_State* l) {
    constexpr size_t alignment = alignof(T);
    // We know lua_newuserdata aligns allocations to 8, so we need a
    // padding of at most alignment - 8 to find a sufficiently aligned
    // address.
    static_assert(alignment>= 8);
    constexpr size_t pad = alignment - 8;
    char* p = reinterpret_cast<char*>(lua_newuserdata(l, sizeof(T) + pad));
    return reinterpret_cast<T*>(align_up(p, alignment));
}

static void push_big_decimal(lua_State* l, const big_decimal& v) {
    auto* p = aligned_used_data<big_decimal>(l);
    new (p) big_decimal(v);
    luaL_setmetatable(l, scylla_decimal_metatable_name);
}

static void push_cpp_int(lua_State* l, const utils::multiprecision_int& v) {
    push_big_decimal(l, big_decimal(0, v));
}

struct lua_table {
};

template <typename Func>
using lua_visit_ret_type = std::invoke_result_t<Func, const double&>;

template <typename Func>
concept CanHandleRawLuaTypes = requires(Func f) {
    { f(*static_cast<const long long*>(nullptr)) }                      -> std::same_as<lua_visit_ret_type<Func>>;
    { f(*static_cast<const double*>(nullptr)) }                         -> std::same_as<lua_visit_ret_type<Func>>;
    { f(*static_cast<const big_decimal*>(nullptr)) }                    -> std::same_as<lua_visit_ret_type<Func>>;
    { f(*static_cast<const std::string_view*>(nullptr)) }               -> std::same_as<lua_visit_ret_type<Func>>;
    { f(*static_cast<const lua_table*>(nullptr)) }                      -> std::same_as<lua_visit_ret_type<Func>>;
};

template <typename Func>
requires CanHandleRawLuaTypes<Func>
static auto visit_lua_raw_value(lua_State* l, int index, Func&& f) {
    switch (lua_type(l, index)) {
    case LUA_TNONE:
        assert(0 && "Invalid index");
    case LUA_TNUMBER:
        if (lua_isinteger(l, index)) {
            return f(lua_tointeger(l, index));
        }
        return f(lua_tonumber(l, index));
    case LUA_TSTRING: {
        size_t len;
        const char* s = lua_tolstring(l, index, &len);
        return f(std::string_view{s, len});
    }
    case LUA_TTABLE:
        return f(lua_table{});
    case LUA_TBOOLEAN:
    case LUA_TFUNCTION:
    case LUA_TNIL:
        throw exceptions::invalid_request_exception("unexpected value");
    case LUA_TUSERDATA:
        return f(*get_decimal(l, index));
    case LUA_TTHREAD:
    case LUA_TLIGHTUSERDATA:
        assert(0 && "We never make thread or light user data visible to scripts");
    }
    assert(0 && "invalid lua type");
}

template <typename Func>
static auto visit_decimal(const big_decimal &v, Func&& f) {
    boost::multiprecision::cpp_rational r = v.as_rational();
    const boost::multiprecision::cpp_int& dividend = numerator(r);
    const boost::multiprecision::cpp_int& divisor = denominator(r);
    if (dividend % divisor == 0) {
        return f(utils::multiprecision_int(dividend/divisor));
    }
    return f(r.convert_to<double>());
}

template <typename Func>
concept CanHandleLuaTypes = requires(Func f) {
    { f(*static_cast<const double*>(nullptr)) }                         -> std::same_as<lua_visit_ret_type<Func>>;
    { f(*static_cast<const utils::multiprecision_int*>(nullptr)) }      -> std::same_as<lua_visit_ret_type<Func>>;
    { f(*static_cast<const big_decimal*>(nullptr)) }                    -> std::same_as<lua_visit_ret_type<Func>>;
    { f(*static_cast<const std::string_view*>(nullptr)) }               -> std::same_as<lua_visit_ret_type<Func>>;
    { f(*static_cast<const lua_table*>(nullptr)) }                      -> std::same_as<lua_visit_ret_type<Func>>;
};

// This is used to test if a double fits in a long long, so
// we expect overflows. Prevent the sanitizer from complaining.
#ifdef __clang__
[[clang::no_sanitize("undefined")]]
#endif
static
long long
cast_to_long_long_allow_overflow(double v) {
    return (long long)v;
}

template <typename Func>
requires CanHandleLuaTypes<Func>
static auto visit_lua_value(lua_State* l, int index, Func&& f) {
    struct visitor {
        lua_State* l;
        int index;
        Func& f;
        auto operator()(const long long& v) { return f(utils::multiprecision_int(v)); }
        auto operator()(const utils::multiprecision_int& v) { return f(v); }
        auto operator()(const double& v) {
            long long v2 = cast_to_long_long_allow_overflow(v);
            if (v2 == v) {
                return (*this)(v2);
            }
            // FIXME: We could use frexp to produce a decimal instead of a double
            return f(v);
        }
        auto operator()(const std::string_view& v) {
            big_decimal v2;
            try {
                v2 = big_decimal(v);
            } catch (marshal_exception&) {
                // The string is not a valid big_decimal. Let Lua try to convert it to a double.
                int isnum;
                double d = lua_tonumberx(l, index, &isnum);
                if (isnum) {
                    return (*this)(d);
                }
                return f(v);
            }
            return (*this)(v2);
        }
        auto operator()(const big_decimal& v) {
            struct visitor {
                Func& f;
                const big_decimal &d;
                auto operator()(const double&) { return f(d); }
                auto operator()(const utils::multiprecision_int& v) { return f(v); }
            };
            return visit_decimal(v, visitor{f, v});
        }
        auto operator()(const lua_table& v) {
            return f(v);
        }
    };
    return visit_lua_raw_value(l, index, visitor{l, index, f});
}

template <typename Func>
static auto visit_lua_number(lua_State* l, int index, Func&& f) {
    return visit_lua_value(l, index, make_visitor(
               [] (const std::string_view& v) -> std::invoke_result_t<Func, double> {
                   throw exceptions::invalid_request_exception("value is not a number");
               },
               [] (const lua_table&) -> std::invoke_result_t<Func, double> {
                   throw exceptions::invalid_request_exception("value is not a number");
               },
               std::forward<Func>(f)
           ));
}

template <typename Func> static auto visit_lua_decimal(lua_State* l, int index, Func&& f) {
    return visit_lua_number(l, index, make_visitor(
               [&f](const utils::multiprecision_int& v) { return f(big_decimal(0, v)); },
               [&f](const auto& v) { return f(v); }
           ));
}

static int decimal_gc(lua_State *l) {
    std::destroy_at(get_decimal(l, 1));
    return 0;
}

static double decimal_to_double(const big_decimal &d) {
    return visit_decimal(d, [] (auto&& v) { return double(v); });
}

static const big_decimal& get_decimal_in_binary_op(lua_State* l) {
    auto* a = get_decimal(l, 1);
    if (a == nullptr) {
        lua_insert(l, 1);
        a = get_decimal(l, 1);
        assert(a);
    }
    return *a;
}

template<typename Func>
static void visit_decimal_bin_op(lua_State* l, Func&& F) {
    const auto& a = get_decimal_in_binary_op(l);
    struct bin_op_visitor {
        const big_decimal& a;
        Func& F;
        lua_State* l;
        void operator()(const double& b) {
            lua_pushnumber(l, F(decimal_to_double(a), b));
        }
        void operator()(const big_decimal& b) {
            push_big_decimal(l, F(a, b));
        }
    };

    visit_lua_decimal(l, -1, bin_op_visitor{a, F, l});
}

static int decimal_add(lua_State* l) {
    visit_decimal_bin_op(l, [](auto&& a, auto&& b) { return a + b; });
    return 1;
}

static int decimal_sub(lua_State* l) {
    visit_decimal_bin_op(l, [](auto&& a, auto&& b) { return a - b; });
    return 1;
}

static const struct luaL_Reg decimal_methods[] {
    {"__gc", decimal_gc},
    {"__add", decimal_add},
    {"__sub", decimal_sub},
    {nullptr, nullptr}
};

static int load_script_l(lua_State* l) {
    const auto& bitcode = *reinterpret_cast<lua::bitcode_view*>(lua_touserdata(l, 1));
    const auto& binary = bitcode.bitcode;

    for (const luaL_Reg* lib = loadedlibs; lib->func; lib++) {
        luaL_requiref(l, lib->name, lib->func, 1);
        lua_pop(l, 1);
    }

    luaL_newmetatable(l, scylla_decimal_metatable_name);
    lua_pushvalue(l, -1);
    lua_setfield(l, -2, "__index");
    luaL_setfuncs(l, decimal_methods, 0);

    if (luaL_loadbufferx(l, binary.data(), binary.size(), "<internal>", "b")) {
        lua_error(l);
    }

    return 1;
}

static lua_slice_state load_script(const lua::runtime_config& cfg, lua::bitcode_view binary) {
    lua_slice_state l = new_lua(cfg);

    // Run the initialization from lua_pcall so we don't have to
    // handle longjmp. We know that a new state has a few reserved
    // stack slots and the following push calls don't allocate.
    lua_pushcfunction(l, load_script_l);
    lua_pushlightuserdata(l, &binary);
    if (lua_pcall(l, 1, 1, 0)) {
        throw std::runtime_error(std::string("could not initiate: ") + lua_tostring(l, -1));
    }

    return l;
}

using millisecond = std::chrono::duration<double, std::milli>;
static auto now() { return std::chrono::system_clock::now(); }

static utils::multiprecision_int get_varint(lua_State* l, int index) {
    return visit_lua_number(l, index, make_visitor(
               [](const utils::multiprecision_int& v) { return v; },
               [](const auto& v) -> utils::multiprecision_int{
                   throw exceptions::invalid_request_exception("value is not an integer");
               }
           ));
}

static sstring get_string(lua_State *l, int index) {
    return visit_lua_value(l, index,  make_visitor(
        [] (const lua_table&) -> sstring {
            throw exceptions::invalid_request_exception("unexpected value");
        },
        [] (const utils::multiprecision_int& p) {
            return sstring(p.str());
        },
        [] (const auto& v) {
            return format("{}", v);
        }));
}

static data_value convert_from_lua(lua_slice_state &l, const data_type& type);

namespace {
struct lua_date_table {
    // The lua date table is documented at https://www.lua.org/pil/22.1.html
    // date::year uses a int64_t, but there is no reason to try to
    // support 64 bit years. In practice the limitations are
    // * year_month_day::to_days hits a signed integer overflow for
    //   large years.
    // * boost::gregorian only supports the years [1400,9999]
    int32_t year;

    // Both date::month and date::day use unsigned char.
    unsigned char month;
    unsigned char day;
    std::optional<int32_t> hour;
    std::optional<int32_t> minute;
    std::optional<int32_t> second;
};

static lua_date_table get_lua_date_table(lua_State* l, int index) {
    std::optional<int32_t> year;
    std::optional<unsigned char> month;
    std::optional<unsigned char> day;
    std::optional<int32_t> hour;
    std::optional<int32_t> minute;
    std::optional<int32_t> second;

    lua_pushnil(l);
    while (lua_next(l, index - 1) != 0) {
        auto k = get_string(l, index - 1);
        auto v = get_varint(l, index);
        lua_pop(l, 1);
        if (k == "month") {
            month = (unsigned char)v;
            if (*month != v) {
                throw exceptions::invalid_request_exception(format("month is too large: '{}'", v.str()));
            }
        } else if (k == "day") {
            day = (unsigned char)v;
            if (*day != v) {
                throw exceptions::invalid_request_exception(format("day is too large: '{}'", v.str()));
            }
        } else {
            int32_t vint(v);
            if (vint != v) {
                throw exceptions::invalid_request_exception(format("{} is too large: '{}'", k, v.str()));
            }
            if (k == "year") {
                year = vint;
            } else if (k == "hour") {
                hour = vint;
            } else if (k == "min") {
                minute = vint;
            } else if (k == "sec") {
                second = vint;
            } else {
                throw exceptions::invalid_request_exception(format("invalid date table field: '{}'", k));
            }
        }
    }
    if (!year || !month || !day) {
        throw exceptions::invalid_request_exception("date table must have year, month and day");
    }
    return lua_date_table{*year, *month, *day, hour, minute, second};
}

struct simple_date_return_visitor {
    lua_slice_state& l;
    template <typename T>
    uint32_t operator()(const T&) {
        throw exceptions::invalid_request_exception("date must be a string, integer or date table");
    }
    uint32_t operator()(const utils::multiprecision_int& v) {
        if (v > std::numeric_limits<uint32_t>::max()) {
            throw exceptions::invalid_request_exception("date value must fit in 32 bits");
        }
        return uint32_t(v);
    }
    uint32_t operator()(const std::string_view& v) {
        return simple_date_type_impl::from_sstring(v);
    }
    uint32_t operator()(const lua_table&);
};

struct timestamp_return_visitor {
    lua_slice_state& l;
    template <typename T>
    db_clock::time_point operator()(const T&) {
        throw exceptions::invalid_request_exception("timestamp must be a string, integer or date table");
    }
    db_clock::time_point operator()(const utils::multiprecision_int& v) {
        int64_t v2 = int64_t(v);
        if (v2 == v) {
            return db_clock::time_point(db_clock::duration(v2));
        }
        throw exceptions::invalid_request_exception("timestamp value must fit in signed 64 bits");
    }
    db_clock::time_point operator()(const std::string_view& v) {
        return timestamp_type_impl::from_sstring(v);
    }
    db_clock::time_point operator()(const lua_table&);
};

struct from_lua_visitor {
    lua_slice_state& l;

    data_value operator()(const reversed_type_impl& t) {
        // This is unreachable since reversed_type_impl is used only
        // in the tables. The function return the underlying type.
        abort();
    }

    data_value operator()(const empty_type_impl& t) {
        // This is unreachable since empty types are not user visible.
        abort();
    }

    data_value operator()(const decimal_type_impl& t) {
        struct visitor {
            big_decimal operator()(const double& b) {
                throw exceptions::invalid_request_exception("value is not a decimal");
            }
            big_decimal operator()(const big_decimal& b) {
                return b;
            }
        };
        return visit_lua_decimal(l, -1, visitor{});
    }

    data_value operator()(const varint_type_impl& t) {
        return get_varint(l, -1);
    }

    data_value operator()(const duration_type_impl& t) {
        return visit_lua_value(l, -1, make_visitor(
            [] (const auto&) -> cql_duration {
                throw exceptions::invalid_request_exception("a duration must be of the form { months = v1, days = v2, nanoseconds = v3 }");
            },
            [] (const std::string_view& v) {
                return cql_duration(v);
            },
            [this] (const lua_table&) {
                int32_t months = 0;
                int32_t days = 0;
                int64_t nanoseconds = 0;
                lua_pushnil(l);
                while (lua_next(l, -2) != 0) {
                    auto k = get_string(l, -2);
                    auto v = get_varint(l, -1);
                    lua_pop(l, 1);
                    if (k == "months") {
                        months = int32_t(v);
                        if (v != months) {
                            throw exceptions::invalid_request_exception(format("{} months doesn't fit in a 32 bit integer", v.str()));
                        }
                    } else if (k == "days") {
                        days = int32_t(v);
                        if (v != days) {
                            throw exceptions::invalid_request_exception(format("{} days doesn't fit in a 32 bit integer", v.str()));
                        }
                    } else if (k == "nanoseconds") {
                        nanoseconds = int64_t(v);
                        if (v != nanoseconds) {
                            throw exceptions::invalid_request_exception(format("{} nanoseconds doesn't fit in a 64 bit integer", v.str()));
                        }
                    } else {
                        throw exceptions::invalid_request_exception(format("invalid duration field: '{}'", k));
                    }
                }
                return cql_duration(months_counter(months), days_counter(days), nanoseconds_counter(nanoseconds));
            }));
    }

    data_value operator()(const set_type_impl& t) {
        std::vector<data_value> elements;
        const data_type& element_type = t.get_elements_type();
        lua_pushnil(l);
        while (lua_next(l, -2) != 0) {
            if (!lua_toboolean(l, -1)) {
                throw exceptions::invalid_request_exception("sets are represented with tables with true values");
            }
            lua_pop(l, 1);
            elements.push_back(convert_from_lua(l, element_type));
        }
        std::sort(elements.begin(), elements.end(), [&](const data_value& a, const data_value& b) {
            // FIXME: this is madness, we have to be able to compare without serializing!
            return element_type->less(a.serialize_nonnull(), b.serialize_nonnull());
        });
        return make_set_value(t.shared_from_this(), std::move(elements));
    }

    data_value operator()(const map_type_impl& t) {
        const data_type& key_type = t.get_keys_type();
        const data_type& value_type = t.get_values_type();
        using map_pair = std::pair<data_value, data_value>;
        std::vector<map_pair> elements;
        lua_pushnil(l);
        while (lua_next(l, -2) != 0) {
            auto v = convert_from_lua(l, value_type);
            lua_pop(l, 1);
            auto k = convert_from_lua(l, key_type);
            elements.push_back({k, v});
        }
        std::sort(elements.begin(), elements.end(), [&](const map_pair& a, const map_pair& b) {
            // FIXME: this is madness, we have to be able to compare without serializing!
            return key_type->less(a.first.serialize_nonnull(), b.first.serialize_nonnull());
        });
        return make_map_value(t.shared_from_this(), std::move(elements));
    }

    data_value operator()(const list_type_impl& t) {
        if (!lua_istable(l, -1)) {
            throw exceptions::invalid_request_exception("value is not a table");
        }

        const data_type& elements_type = t.get_elements_type();
        using table_pair = std::pair<utils::multiprecision_int, data_value>;
        std::vector<table_pair> pairs;
        lua_pushnil(l);
        while (lua_next(l, -2) != 0) {
            auto v = convert_from_lua(l, elements_type);
            lua_pop(l, 1);
            pairs.push_back({get_varint(l, -1), v});
        }

        std::sort(pairs.begin(), pairs.end(), [] (const table_pair& a, const table_pair& b) {
            return a.first < b.first;
        });

        size_t num_elements = pairs.size();
        std::vector<data_value> elements;
        for (size_t i = 0; i < num_elements; ++i) {
            if (utils::multiprecision_int(i + 1) != pairs[i].first) {
                throw exceptions::invalid_request_exception("table is not a sequence");
            }
            elements.push_back(pairs[i].second);
        }
        return make_list_value(t.shared_from_this(), std::move(elements));
    }

    data_value operator()(const tuple_type_impl& t) {
        if (!lua_istable(l, -1)) {
            throw exceptions::invalid_request_exception("value is not a table");
        }

        size_t num_elements = t.size();
        std::vector<std::optional<data_value>> opt_elements(num_elements);

        lua_pushnil(l);
        while (lua_next(l, -2) != 0) {
            auto k_varint = get_varint(l, -2);
            if (k_varint > num_elements || k_varint < 1) {
                throw exceptions::invalid_request_exception(
                        format("key {} is not valid for a sequence of size {}", k_varint.str(), num_elements));
            }
            size_t k = size_t(k_varint);
            opt_elements[k - 1] = convert_from_lua(l, t.type(k - 1));
            lua_pop(l, 1);
        }

        std::vector<data_value> elements;
        elements.reserve(num_elements);
        for (size_t i = 0; i < num_elements; ++i) {
            if (!opt_elements[i]) {
                throw exceptions::invalid_request_exception(
                        format("key {} missing in sequence of size {}", i + 1, num_elements));
            }
            elements.push_back(*opt_elements[i]);
        }
        return make_tuple_value(t.shared_from_this(), std::move(elements));
    }

    data_value operator()(const user_type_impl& t) {
        size_t num_fields = t.field_types().size();

        std::unordered_map<sstring, std::pair<unsigned, data_type>> field_types;
        field_types.reserve(num_fields);
        for (unsigned i = 0; i < num_fields; ++i) {
            field_types.insert({t.field_name_as_string(i), {i, t.field_type(i)}});
        }

        std::vector<std::optional<data_value>> opt_elements(num_fields);
        lua_pushnil(l);
        while (lua_next(l, -2) != 0) {
            auto s = get_string(l, -2);
            auto iter = field_types.find(s);
            if (iter == field_types.end()) {
                throw exceptions::invalid_request_exception(format("invalid UDT field '{}'", s));
            }

            const auto &p = iter->second;
            auto v = convert_from_lua(l, p.second);
            lua_pop(l, 1);

            opt_elements[p.first] = std::move(v);
        }

        std::vector<data_value> elements;
        elements.reserve(num_fields);
        for (size_t i = 0; i < num_fields; ++i) {
            if (!opt_elements[i]) {
                throw exceptions::invalid_request_exception(
                        format("key {} missing in udt {}", t.field_name_as_string(i), t.get_name_as_string()));
            }
            elements.push_back(*opt_elements[i]);
        }

        return make_user_value(t.shared_from_this(), std::move(elements));
    }

    data_value operator()(const inet_addr_type_impl& t) {
        return t.from_sstring(get_string(l, -1));
    }

    data_value operator()(const uuid_type_impl&) {
        return uuid_type_impl::from_sstring(get_string(l, -1));
    }

    data_value operator()(const timeuuid_type_impl&) {
        return timeuuid_native_type{timeuuid_type_impl::from_sstring(get_string(l, -1))};
    }

    data_value operator()(const bytes_type_impl& t) {
        sstring v = get_string(l, -1);
        return data_value(bytes(reinterpret_cast<const int8_t*>(v.data()), v.size()));
    }

    data_value operator()(const utf8_type_impl& t) {
        sstring s = get_string(l, -1);
        auto error_pos = utils::utf8::validate_with_error_position(reinterpret_cast<uint8_t*>(s.data()), s.size());
        if (error_pos) {
            throw exceptions::invalid_request_exception(format("value is not valid utf8, invalid character at byte offset {}", *error_pos));
        }
        return std::move(s);
    }

    data_value operator()(const ascii_type_impl& t) {
        sstring s = get_string(l, -1);
        if (utils::ascii::validate(reinterpret_cast<uint8_t*>(s.data()), s.size())) {
            return ascii_native_type{std::move(s)};
        }
        throw exceptions::invalid_request_exception("value is not valid ascii");
    }

    data_value operator()(const boolean_type_impl& t) {
        return bool(lua_toboolean(l, -1));
    }

    template <typename T> data_value operator()(const floating_type_impl<T>& t) {
        return visit_lua_number(l, -1, make_visitor(
                   [] (const big_decimal& v) -> T { return decimal_to_double(v); },
                   [] (const auto& v) { return T(v); }
               ));
    }

    int64_t get_integer() {
        return from_varint_to_integer(get_varint(l, -1));
    }

    data_value operator()(const timestamp_date_base_class& t) {
        return visit_lua_value(l, -1, timestamp_return_visitor{l});
    }

    data_value operator()(const time_type_impl& t) {
        return time_native_type{visit_lua_value(l, -1, make_visitor(
                   [] (const auto&) -> int64_t {
                       throw exceptions::invalid_request_exception("time must be a string or an integer");
                   },
                   [] (const utils::multiprecision_int& v) {
                       int64_t v2 = int64_t(v);
                       if (v2 == v) {
                           return v2;
                       }
                       throw exceptions::invalid_request_exception("time value must fit in signed 64 bits");
                   },
                   [] (const std::string_view& v) {
                       return time_type_impl::from_sstring(v);
                   }
               ))};
    }

    data_value operator()(const counter_type_impl&) {
        // No data_value ever has a counter type, it is represented
        // with long_type instead.
        return get_integer();
    }

    template <typename T> data_value operator()(const integer_type_impl<T>& t) {
        return T(get_integer());
    }

    data_value operator()(const simple_date_type_impl& t) {
        return simple_date_native_type{visit_lua_value(l, -1, simple_date_return_visitor{l})};
    }
};

uint32_t simple_date_return_visitor::operator()(const lua_table&) {
    auto table = get_lua_date_table(l, -1);
    if (table.hour || table.minute || table.second) {
        throw exceptions::invalid_request_exception("date type has no hour, minute or second");
    }
    date::year_month_day ymd{date::year{table.year}, date::month{table.month}, date::day{table.day}};
    int64_t days = date::local_days(ymd).time_since_epoch().count() + (1UL << 31);
    return (*this)(utils::multiprecision_int(days));
}

db_clock::time_point timestamp_return_visitor::operator()(const lua_table&) {
    auto table = get_lua_date_table(l, -1);
    boost::gregorian::date date(table.year, table.month, table.day);
    boost::posix_time::time_duration time(table.hour.value_or(12), table.minute.value_or(0), table.second.value_or(0));
    boost::posix_time::ptime timestamp(date, time);
    int64_t msec = (timestamp - boost::posix_time::from_time_t(0)).total_milliseconds();
    return (*this)(utils::multiprecision_int(msec));
}
}

static data_value convert_from_lua(lua_slice_state &l, const data_type& type) {
    if (lua_isnil(l, -1)) {
        return data_value::make_null(type);
    }
    return ::visit(*type, from_lua_visitor{l});
}

static bytes_opt convert_return(lua_slice_state &l, const data_type& return_type) {
    int num_return_vals = lua_gettop(l);
    if (num_return_vals != 1) {
        throw exceptions::invalid_request_exception(
            format("{} values returned, expected {}", num_return_vals, 1));
    }

    // FIXME: It should be possible to avoid creating the data_value,
    // or even better, change the function::execute interface to
    // return a data_value instead of bytes_opt.
    return convert_from_lua(l, return_type).serialize();
}

static void push_sstring(lua_slice_state& l, const sstring& v) {
    lua_pushlstring(l, v.c_str(), v.size());
}

static void push_argument(lua_slice_state& l, const data_value& arg);

namespace {
struct to_lua_visitor {
    lua_slice_state& l;

    void operator()(const varint_type_impl& t, const emptyable<utils::multiprecision_int>* v) {
        push_cpp_int(l, *v);
    }

    void operator()(const decimal_type_impl& t, const emptyable<big_decimal>* v) {
        push_big_decimal(l, *v);
    }

    void operator()(const counter_type_impl& t, const void* v) {
        // This is unreachable since deserialize_visitor for
        // counter_type_impl return a long.
        abort();
    }

    void operator()(const empty_type_impl& t, const void* v) {
        // This is unreachable since empty types are not user visible.
        abort();
    }

    void operator()(const reversed_type_impl& t, const void* v) {
        // This is unreachable since reversed_type_impl is used only
        // in the tables. The function gets the underlying type.
        abort();
    }

    void operator()(const map_type_impl& t, const std::vector<std::pair<data_value, data_value>>* v) {
        // returns the table { k1 = v1, k2 = v2, ...}
        lua_createtable(l, 0, v->size());
        for (const auto& p : *v) {
            push_argument(l, p.first);
            push_argument(l, p.second);
            lua_rawset(l, -3);
        }
    }

    void operator()(const user_type_impl& t, const std::vector<data_value>* v) {
        // returns the table { field1 = v1, field2 = v2, ...}
        lua_createtable(l, 0, v->size());
        for (int i = 0, n = v->size(); i < n; ++i) {
            push_sstring(l, t.field_name_as_string(i));
            push_argument(l, (*v)[i]);
            lua_rawset(l, -3);
        }
    }

    void operator()(const set_type_impl& t, const std::vector<data_value>* v) {
        // returns the table { v1 = true, v2 = true, ...}
        lua_createtable(l, 0, v->size());
        for (const data_value& dv : *v) {
            push_argument(l, dv);
            lua_pushboolean(l, true);
            lua_rawset(l, -3);
        }
    }

    template <typename T>
    void operator()(const concrete_type<std::vector<data_value>, T>& t, const std::vector<data_value>* v) {
        // returns the table {v1, v2, ...}
        lua_createtable(l, v->size(), 0);
        int i = 0;
        for (const data_value& dv : *v) {
            push_argument(l, dv);
            lua_rawseti(l, -2, ++i);
        }
    }

    void operator()(const boolean_type_impl& t, const emptyable<bool>* v) {
        lua_pushboolean(l, *v);
    }

    template <typename T>
    void operator()(const floating_type_impl<T>& t, const emptyable<T>* v) {
        // floats are converted to double
        lua_pushnumber(l, *v);
    }

    template <typename T>
    void operator()(const integer_type_impl<T>& t, const emptyable<T>* v) {
        // Integers are converted to 64 bits
        lua_pushinteger(l, *v);
    }

    void operator()(const bytes_type_impl& t, const bytes* v) {
        // lua strings can hold arbitrary blobs
        lua_pushlstring(l, reinterpret_cast<const char*>(v->c_str()), v->size());
    }

    void operator()(const string_type_impl& t, const sstring* v) {
        push_sstring(l, *v);
    }

    void operator()(const time_type_impl& t, const emptyable<int64_t>* v) {
        // nanoseconds since midnight
        lua_pushinteger(l, *v);
    }

    void operator()(const timestamp_date_base_class& t, const timestamp_date_base_class::native_type* v) {
        // milliseconds since epoch
        lua_pushinteger(l, v->get().time_since_epoch().count());
    }

    void operator()(const simple_date_type_impl& t, const emptyable<uint32_t>* v) {
        // number of days since epoch + 2^31
        lua_pushinteger(l, *v);
    }

    void operator()(const duration_type_impl& t, const emptyable<cql_duration>* v) {
        // returns the table { months = v1, days = v2, nanoseconds = v3 }
        const cql_duration& d = v->get();
        lua_createtable(l, 3, 0);

        lua_pushinteger(l, d.months);
        lua_setfield(l, -2, "months");

        lua_pushinteger(l, d.days);
        lua_setfield(l, -2, "days");

        lua_pushinteger(l, d.nanoseconds);
        lua_setfield(l, -2, "nanoseconds");
    }

    void operator()(const inet_addr_type_impl& t, const emptyable<seastar::net::inet_address>* v) {
        // returns a string
        sstring s = inet_addr_type_impl::to_sstring(v->get());
        push_sstring(l, s);
    }

    void operator()(const concrete_type<utils::UUID>&, const emptyable<utils::UUID>* v) {
        // returns a string
        push_sstring(l, v->get().to_sstring());
    }
};
}

static void push_argument(lua_slice_state& l, const data_value& arg) {
    if (arg.is_null()) {
        lua_pushnil(l);
        return;
    }
    ::visit(arg, to_lua_visitor{l});
}

lua::runtime_config lua::make_runtime_config(const db::config& config) {
    utils::updateable_value<unsigned> max_bytes(config.user_defined_function_allocation_limit_bytes);
    utils::updateable_value<unsigned> max_contiguous(config.user_defined_function_contiguous_allocation_limit_bytes());
    utils::updateable_value<unsigned> timeout_in_ms(config.user_defined_function_time_limit_ms());

    return lua::runtime_config{std::move(timeout_in_ms), std::move(max_bytes), std::move(max_contiguous)};
}

// run the script for at most max_instructions
future<bytes_opt> lua::run_script(lua::bitcode_view bitcode, const std::vector<data_value>& values, data_type return_type, const lua::runtime_config& cfg) {
    lua_slice_state l = load_script(cfg, bitcode);
    unsigned nargs = values.size();
    if (!lua_checkstack(l, nargs)) {
        throw std::runtime_error("could push args to the stack");
    }
    for (const data_value& arg : values) {
        push_argument(l, arg);
    }

    // We don't update the timeout once we start executing the function
    using millisecond = std::chrono::duration<double, std::milli>;
    using duration = std::chrono::system_clock::duration;
    duration elapsed{0};
    duration timeout = std::chrono::duration_cast<duration>(millisecond(cfg.timeout_in_ms));
    return repeat_until_value([l = std::move(l), elapsed, return_type, nargs, timeout = std::move(timeout)] () mutable {
        // Set the hook before resuming. We have to do it here since the hook can reset itself
        // if it detects we are spending too much time in C.
        // The hook will be called after 1000 instructions.
        lua_sethook(l, debug_hook, LUA_MASKCALL | LUA_MASKCOUNT, 1000);
        auto start = ::now();
        LUA_504_PLUS(int nresults;)
        switch (lua_resume(l, nullptr, nargs LUA_504_PLUS(, &nresults))) {
        case LUA_OK:
            return make_ready_future<std::optional<bytes_opt>>(convert_return(l, return_type));
        case LUA_YIELD: {
            nargs = 0;
            elapsed += ::now() - start;
            if (elapsed > timeout) {
                millisecond ms = elapsed;
                throw exceptions::invalid_request_exception(format("lua execution timeout: {}ms elapsed", ms.count()));
            }
            return make_ready_future<std::optional<bytes_opt>>(std::nullopt);
        }
        default:
            throw exceptions::invalid_request_exception(std::string("lua execution failed: ") +
                                                        lua_tostring(l, -1));
        }
    });
}
