/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include <boost/algorithm/string/join.hpp>
#include <fmt/chrono.h>
#include <lua.hpp>
#include <seastar/core/fstream.hh>
#include <seastar/core/seastar.hh>
#include <seastar/coroutine/maybe_yield.hh>
#include <seastar/util/short_streams.hh>

#include "lang/lua_scylla_types.hh"
#include "reader_permit.hh"
#include "schema/schema.hh"
#include "sstables/sstables.hh"
#include "tools/json_writer.hh"
#include "tools/lua_sstable_consumer.hh"
#include "types/collection.hh"
#include "types/tuple.hh"

namespace {

class gc_clock_time_point;
class counter_shards_value;
class atomic_cell_with_type;
class collection_with_type;
class json_writer;

template <typename T> struct type_to_metatable { static constexpr const char* metatable_name = nullptr; };
template <> struct type_to_metatable<column_definition> { static constexpr const char* metatable_name = "Scylla.column_definition"; };
template <> struct type_to_metatable<schema> { static constexpr const char* metatable_name = "Scylla.schema"; };
template <> struct type_to_metatable<partition_key> { static constexpr const char* metatable_name = "Scylla.partition_key"; };
template <> struct type_to_metatable<clustering_key> { static constexpr const char* metatable_name = "Scylla.clustering_key"; };
template <> struct type_to_metatable<dht::ring_position_ext> { static constexpr const char* metatable_name = "Scylla.ring_position"; };
template <> struct type_to_metatable<position_in_partition> { static constexpr const char* metatable_name = "Scylla.position_in_partition"; };
template <> struct type_to_metatable<sstables::sstable> { static constexpr const char* metatable_name = "Scylla.sstable"; };
template <> struct type_to_metatable<partition_start> { static constexpr const char* metatable_name = "Scylla.partition_start"; };
template <> struct type_to_metatable<gc_clock_time_point> { static constexpr const char* metatable_name = "Scylla.gc_clock_time_point"; };
template <> struct type_to_metatable<tombstone> { static constexpr const char* metatable_name = "Scylla.tombstone"; };
template <> struct type_to_metatable<data_value> { static constexpr const char* metatable_name = "Scylla.data_value"; };
template <> struct type_to_metatable<counter_shards_value> { static constexpr const char* metatable_name = "Scylla.counter_shards_value"; };
template <> struct type_to_metatable<atomic_cell_with_type> { static constexpr const char* metatable_name = "Scylla.atomic_cell"; };
template <> struct type_to_metatable<collection_with_type> { static constexpr const char* metatable_name = "Scylla.collection"; };
template <> struct type_to_metatable<static_row> { static constexpr const char* metatable_name = "Scylla.static_row"; };
template <> struct type_to_metatable<row_marker> { static constexpr const char* metatable_name = "Scylla.row_marker"; };
template <> struct type_to_metatable<clustering_row> { static constexpr const char* metatable_name = "Scylla.clustering_row"; };
template <> struct type_to_metatable<range_tombstone_change> { static constexpr const char* metatable_name = "Scylla.range_tombstone_change"; };
template <> struct type_to_metatable<json_writer> { static constexpr const char* metatable_name = "Scylla.json_writer"; };

template <typename T>
const char* get_metatable_name() {
    const auto metatable_name = type_to_metatable<std::remove_cv_t<T>>::metatable_name;
    assert(metatable_name);
    return metatable_name;
}

struct lua_closer {
    void operator()(lua_State* l) {
        lua_close(l);
    }
};

void* lua_alloc(void* ud, void* ptr, size_t osize, size_t nsize) {
    if (!nsize) {
        free(ptr);
        return nullptr;
    } else {
        return realloc(ptr, nsize);
    }
}

static const luaL_Reg loadedlibs[] = {
    {"base", luaopen_base},
    {LUA_STRLIBNAME, luaopen_string},
    {LUA_TABLIBNAME, luaopen_table},
    {NULL, NULL},
};

template <typename T>
void push_userdata_ref(lua_State* l, T& v) {
    *reinterpret_cast<T**>(lua_newuserdata(l, sizeof(T*))) = &v;
    luaL_setmetatable(l, get_metatable_name<T>());
}

template <typename T>
T& pop_userdata_ref(lua_State* l, int i) {
    return **reinterpret_cast<T**>(luaL_checkudata(l, i, get_metatable_name<T>()));
}

template <typename T>
void* alloc_userdata(lua_State* l) {
    if constexpr (alignof(T) > 8) {
        return lua::aligned_user_data<T>(l);
    } else {
        return lua_newuserdata(l, sizeof(T));
    }
}

template <typename T, typename... Arg>
void push_userdata(lua_State* l, Arg&&... arg) {
    new (alloc_userdata<T>(l)) T(std::forward<Arg>(arg)...);
    luaL_setmetatable(l, get_metatable_name<T>());
}

// Push userdata with default constructed T in it.
template <typename T>
void push_userdata(lua_State* l) {
    new (alloc_userdata<T>(l)) T;
    luaL_setmetatable(l, get_metatable_name<T>());
}

template <typename T>
T* get_userdata_ptr(lua_State* l, int i) {
    auto p = luaL_checkudata(l, i, get_metatable_name<T>());
    if constexpr (alignof(T) <= 8) {
        return reinterpret_cast<T*>(p);
    } else {
        return align_up(reinterpret_cast<char*>(p), alignof(T));
    }
}

template <typename T>
T pop_userdata(lua_State* l, int i) {
    return std::move(*get_userdata_ptr<T>(l, i));
}

// Get reference to userdata stored by-value.
// Valid only until Lua GC:s the value, so don't keep it around for long.
template <typename T>
T& pop_userdata_as_ref(lua_State* l, int i) {
    return *get_userdata_ptr<T>(l, i);
}

template <typename T>
int cxx_gc_l(lua_State* l) {
    auto& v = pop_userdata_as_ref<T>(l, 1);
    v.~T();
    lua_pop(l, 1);
    return 0;
}

template <typename T>
int tostring_l(lua_State* l) {
    const auto& v = pop_userdata_as_ref<T>(l, 1);
    lua_pop(l, 1);
    lua::push_sstring(l, format("{}", v));
    return 1;
}

const schema& get_schema_l(lua_State* l) {
    lua_getglobal(l, "schema");
    auto& s = pop_userdata_ref<const schema>(l, -1);
    lua_pop(l, 1);
    return s;
}

template <typename T, typename Wrapper>
int tostring_with_wrapper(lua_State* l) {
    const auto& v = pop_userdata_as_ref<T>(l, 1);
    const auto& s = get_schema_l(l);
    lua_pop(l, 1);
    lua::push_sstring(l, format("{}", Wrapper(s, v)));
    return 1;
}

int column_definition_index_l(lua_State* l) {
    const auto& cdef = pop_userdata_ref<const column_definition>(l, 1);
    auto field = lua_tostring(l, 2);
    lua_pop(l, 2);
    if (strcmp(field, "id") == 0) {
        lua_pushinteger(l, cdef.id);
        return 1;
    } else if (strcmp(field, "name") == 0) {
        lua::push_sstring(l, cdef.name_as_text());
        return 1;
    } else if (strcmp(field, "kind") == 0) {
        lua::push_sstring(l, to_sstring(cdef.kind));
        return 1;
    }
    return 0;
}

int push_column_definitions(lua_State* l, schema::const_iterator_range_type cols) {
    lua_createtable(l, std::distance(cols.begin(), cols.end()), 0);
    size_t i = 1; // lua arrays start from 1
    for (const auto& col : cols) {
        push_userdata_ref<const column_definition>(l, col);
        lua_seti(l, -2, i++);
    }
    return 1;
}

int schema_index_l(lua_State* l) {
    const auto& s = pop_userdata_ref<const schema>(l, 1);
    auto field = lua_tostring(l, 2);
    lua_pop(l, 2);
    if (strcmp(field, "partition_key_columns") == 0) {
        return push_column_definitions(l, s.partition_key_columns());
    } else if (strcmp(field, "clustering_key_columns") == 0) {
        return push_column_definitions(l, s.clustering_key_columns());
    } else if (strcmp(field, "static_columns") == 0) {
        return push_column_definitions(l, s.static_columns());
    } else if (strcmp(field, "regular_columns") == 0) {
        return push_column_definitions(l, s.regular_columns());
    } else if (strcmp(field, "all_columns") == 0) {
        return push_column_definitions(l, s.all_columns());
    }
    return 0;
}

template <typename Key>
int key_to_hex_l(lua_State* l) {
    auto& k = pop_userdata_as_ref<Key>(l, 1);
    auto hex = to_hex(to_bytes(k.representation()));
    lua_pop(l, 1);
    lua::push_sstring(l, hex);
    return 1;
}

template <typename Key>
int key_index_l(lua_State* l) {
    auto field = lua_tostring(l, 2);
    const auto& k = pop_userdata_as_ref<Key>(l, 1);
    const auto& s = get_schema_l(l);
    lua_pop(l, 2);
    if (strcmp(field, "components") == 0) {
        const std::vector<data_type>* types;
        if constexpr (std::is_same_v<Key, partition_key>) {
            types = &s.partition_key_type()->types();
        } else {
            types = &s.clustering_key_type()->types();
        }
        const auto values = k.explode(s);
        lua_createtable(l, types->size(), 0);
        for (size_t i = 0; i < types->size(); ++i) {
            if (i == values.size()) {
                // prefix key
                break;
            }
            lua::push_data_value(l, types->at(i)->deserialize(values.at(i)));
            lua_seti(l, -2, i + 1); // lua arrays start from 1
        }
        return 1;
    } else if (strcmp(field, "to_hex") == 0) {
        lua_pushcfunction(l, key_to_hex_l<Key>);
        return 1;
    }
    return 0;
}

int8_t normalize_weight(int w) {
    if (!w) {
        return w;
    } else if (w < 0) {
        return -1;
    } else {
        return 1;
    }
}

int new_ring_position_l(lua_State* l) {
    const auto weight = normalize_weight(lua_tointeger(l, 1));

    const auto& s = get_schema_l(l);

    std::optional<partition_key> pk;
    std::optional<dht::token> token;

    if (lua_gettop(l) == 2) {
        if (lua_type(l, 2) == LUA_TUSERDATA) {
            pk = pop_userdata<partition_key>(l, 2);
            token = dht::get_token(s, *pk);
        } else if (lua_type(l, 2) == LUA_TNUMBER) {
            token = dht::token(dht::token_kind::key, lua_tointeger(l, 2));
        }
    }

    if (lua_gettop(l) == 3) {
        pk = pop_userdata<partition_key>(l, 2);
        if (lua_type(l, 3) == LUA_TNUMBER) {
            token = dht::token(dht::token_kind::key, lua_tointeger(l, 3));
        } else {
            luaL_checktype(l, 3, LUA_TNIL);
        }
    }
    if (!token) {
        if (!weight) {
            luaL_error(l, "weight of 0 is invalid for min/max ring_position");
        }
        token = dht::token(weight < 0 ? dht::token_kind::after_all_keys : dht::token_kind::before_all_keys, 0);
    }

    push_userdata<dht::ring_position_ext>(l, *token, std::move(pk), weight);
    return 1;
}

int tri_cmp_to_int(std::strong_ordering r) {
    if (r < 0) {
        return -1;
    } else if (r == 0) {
        return 0;
    } else {
        return 1;
    }
}

int ring_position_tri_cmp_l(lua_State* l) {
    const auto& a = pop_userdata_as_ref<const dht::ring_position_ext>(l, 1);
    const auto& b = pop_userdata_as_ref<const dht::ring_position_ext>(l, 2);
    lua_pop(l, 2);
    const auto& s = get_schema_l(l);
    lua_pushinteger(l, tri_cmp_to_int(dht::ring_position_tri_compare(s, a, b)));
    return 1;
}

int ring_position_index_l(lua_State* l) {
    const auto& rp = pop_userdata_as_ref<const dht::ring_position_ext>(l, 1);
    auto field = lua_tostring(l, 2);
    lua_pop(l, 2);
    if (strcmp(field, "tri_cmp") == 0) {
        lua_pushcfunction(l, ring_position_tri_cmp_l);
        return 1;
    } else if (strcmp(field, "token") == 0) {
        if (rp.token()._kind == dht::token_kind::key) {
            lua_pushinteger(l, rp.token()._data);
            return 1;
        }
    } else if (strcmp(field, "key") == 0) {
        if (rp.key()) {
            push_userdata<partition_key>(l, *rp.key());
            return 1;
        }
    } else if (strcmp(field, "weight") == 0) {
        lua_pushinteger(l, rp.weight());
        return 1;
    }
    return 0;
}

int position_in_partition_tri_cmp_l(lua_State* l) {
    const auto& a = pop_userdata_as_ref<const position_in_partition>(l, 1);
    const auto& b = pop_userdata_as_ref<const position_in_partition>(l, 2);
    lua_pop(l, 2);
    const auto& s = get_schema_l(l);
    position_in_partition::tri_compare cmp(s);
    lua_pushinteger(l, tri_cmp_to_int(cmp(a, b)));
    return 1;
}

int position_in_partition_index_l(lua_State* l) {
    const auto& pos = pop_userdata_as_ref<const position_in_partition>(l, 1);
    auto field = lua_tostring(l, 2);
    lua_pop(l, 2);
    if (strcmp(field, "tri_cmp") == 0) {
        lua_pushcfunction(l, position_in_partition_tri_cmp_l);
        return 1;
    } else if (strcmp(field, "key") == 0) {
        if (pos.has_key()) {
            push_userdata<clustering_key>(l, pos.key());
            return 1;
        }
    } else if (strcmp(field, "weight") == 0) {
        lua_pushinteger(l, int(pos.get_bound_weight()));
        return 1;
    }
    return 0;
}

int new_position_in_partition_l(lua_State* l) {
    const auto weight = normalize_weight(lua_tointeger(l, 1));

    std::optional<clustering_key> ck;

    if (lua_gettop(l) == 2) {
        if (lua_type(l, 2) == LUA_TUSERDATA) {
            ck = pop_userdata<clustering_key>(l, 2);
        } else {
            luaL_checktype(l, 2, LUA_TNIL);
        }
    }
    if (!ck) {
        if (!weight) {
            luaL_error(l, "weight of 0 is invalid for nil min/max position_in_partition");
        }
    }

    push_userdata<position_in_partition>(l, partition_region::clustered, bound_weight(weight), std::move(ck));
    return 1;
}

template <allow_prefixes AllowPrefix>
int unserialize_key_l(lua_State* l) {
    const auto& s = get_schema_l(l);
    auto key_str = lua_tostring(l, 1);
    lua_pop(l, 2);
    lw_shared_ptr<compound_type<AllowPrefix>> type;
    managed_bytes value;
    try {
        value = managed_bytes(from_hex(key_str));
        if constexpr (AllowPrefix == allow_prefixes::no) {
            type = s.partition_key_type();
        } else {
            type = s.clustering_key_type();
        }
        type->validate(value);
        if constexpr (AllowPrefix == allow_prefixes::no) {
            push_userdata<partition_key>(l, partition_key::from_bytes(value));
        } else {
            push_userdata<clustering_key>(l, clustering_key::from_bytes(value));
        }
        return 1;
    } catch (const std::exception& e) {
        return luaL_error(l, "unserialize partition key: %s", e.what());
    }
}

int token_of_l(lua_State* l) {
    const auto& s = pop_userdata_ref<const schema>(l, 1);
    const auto& k = pop_userdata_as_ref<partition_key>(l, 1);
    lua_pop(l, 2);
    auto t = dht::get_token(s, k);
    lua_pushinteger(l, t._data);
    return 1;
}

int sstable_index_l(lua_State* l) {
    auto field = lua_tostring(l, 2);
    auto& sst = pop_userdata_ref<const sstables::sstable>(l, 1);
    lua_pop(l, 2);
    if (strcmp(field, "filename") == 0) {
        lua::push_sstring(l, sst.get_filename());
        return 1;
    }
    return 0;
}

int partition_start_index_l(lua_State* l) {
    auto field = lua_tostring(l, 2);
    auto& ps = pop_userdata_as_ref<partition_start>(l, 1);
    lua_pop(l, 2);
    if (strcmp(field, "key") == 0) {
        push_userdata<partition_key>(l, ps.key().key());
        return 1;
    } else if (strcmp(field, "token") == 0) {
        lua_pushinteger(l, ps.key().token()._data);
        return 1;
    } else if (strcmp(field, "tombstone") == 0) {
        if (!ps.partition_tombstone()) {
            return 0;
        }
        push_userdata<tombstone>(l, ps.partition_tombstone());
        return 1;
    }
    return 0;
}

void push_gc_clock_time_point(lua_State* l, gc_clock::time_point tp) {
    auto t = gc_clock::to_time_t(tp);
    std::tm tm{};
    // This time is actually UTC, but C API assumes local-times and there is no
    // way to specify the TZ used. So we lie about it being local-time to avoid
    // libC doing TZ adjustments.
    localtime_r(&t, &tm);

    lua_createtable(l, 0, 6);
    luaL_setmetatable(l, get_metatable_name<gc_clock_time_point>());

    lua_pushinteger(l, 1900 + tm.tm_year);
    lua_setfield(l, -2, "year");

    lua_pushinteger(l, 1 + tm.tm_mon);
    lua_setfield(l, -2, "month");

    lua_pushinteger(l, tm.tm_mday);
    lua_setfield(l, -2, "day");

    lua_pushinteger(l, tm.tm_hour);
    lua_setfield(l, -2, "hour");

    lua_pushinteger(l, tm.tm_min);
    lua_setfield(l, -2, "min");

    lua_pushinteger(l, tm.tm_sec);
    lua_setfield(l, -2, "sec");
}

gc_clock::time_point get_gc_clock_time_point_l(lua_State* l) {
    std::tm tm{};
    tm.tm_isdst = -1;

    lua_getfield(l, 1, "year");
    tm.tm_year = lua_tointeger(l, -1) - 1900;
    lua_pop(l, 1);

    lua_getfield(l, 1, "month");
    tm.tm_mon = lua_tointeger(l, -1) - 1;
    lua_pop(l, 1);

    lua_getfield(l, 1, "day");
    tm.tm_mday = lua_tointeger(l, -1);
    lua_pop(l, 1);

    lua_getfield(l, 1, "hour");
    tm.tm_hour = lua_tointeger(l, -1);
    lua_pop(l, 1);

    lua_getfield(l, 1, "min");
    tm.tm_min = lua_tointeger(l, -1);
    lua_pop(l, 1);

    lua_getfield(l, 1, "sec");
    tm.tm_sec = lua_tointeger(l, -1);
    lua_pop(l, 1);

    lua_pop(l, 1);

    return gc_clock::from_time_t(std::mktime(&tm));
}

template <typename Op>
int gc_clock_time_point_binary_op_l(lua_State* l) {
    auto b = get_gc_clock_time_point_l(l);
    lua_pop(l, 1);
    auto a = get_gc_clock_time_point_l(l);
    lua_pop(l, 1);

    Op op;

    lua_pushboolean(l, op(a, b));
    return 1;
}

int gc_clock_time_point_tostring_l(lua_State* l) {
    auto tp = get_gc_clock_time_point_l(l);
    lua::push_sstring(l, format("{:%F %T}z", fmt::gmtime(gc_clock::to_time_t(tp))));
    return 1;
}

int gc_clock_time_point_now_l(lua_State* l) {
    push_gc_clock_time_point(l, gc_clock::now());
    return 1;
}

int gc_clock_time_point_from_string_l(lua_State* l) {
    try {
        auto tp = gc_clock::time_point(gc_clock::duration(timestamp_from_string(lua_tostring(l, 1)) / 1000));
        lua_pop(l, 1);
        push_gc_clock_time_point(l, tp);
    } catch (const std::exception& e) {
        return luaL_error(l, "failed to parse gc_clock_deletion_time from string: %s", e.what());
    }
    return 1;
}

int tombstone_index_l(lua_State* l) {
    auto field = lua_tostring(l, 2);
    auto tomb = pop_userdata<tombstone>(l, 1);
    lua_pop(l, 2);
    if (strcmp(field, "timestamp") == 0) {
        lua_pushinteger(l, tomb.timestamp);
        return 1;
    } else if (strcmp(field, "deletion_time") == 0) {
        push_gc_clock_time_point(l, tomb.deletion_time);
        return 1;
    }
    return 0;
}

int data_value_tostring_l(lua_State* l) {
    auto& dv = pop_userdata_as_ref<data_value>(l, 1);
    lua_pop(l, 1);
    auto type = dv.type();
    if (auto bvopt = dv.serialize()) {
        lua::push_sstring(l, type->to_string(*bvopt));
    } else {
        lua_pushliteral(l, "");
    }
    return 1;
}

int data_value_index_l(lua_State* l) {
    auto field = lua_tostring(l, 2);
    auto& dv = pop_userdata_as_ref<data_value>(l, 1);
    lua_pop(l, 2);
    if (strcmp(field, "value") == 0) {
        lua::push_data_value(l, dv);
        return 1;
    }
    return 0;
}

struct counter_shards_value {
    data_type type;
    managed_bytes data;
};

int counter_shards_value_tostring_l(lua_State* l) {
    auto& csv = pop_userdata_as_ref<counter_shards_value>(l, 1);
    lua_pop(l, 1);
    auto cv = counter_cell_view(atomic_cell_view::from_bytes(*csv.type, csv.data));
    lua::push_sstring(l, format("{}", cv.total_value()));
    return 1;
}

int counter_shards_value_index_l(lua_State* l) {
    auto field = lua_tostring(l, 2);
    auto& csv = pop_userdata_as_ref<counter_shards_value>(l, 1);
    lua_pop(l, 2);
    auto cv = counter_cell_view(atomic_cell_view::from_bytes(*csv.type, csv.data));
    if (strcmp(field, "value") == 0) {
        lua::push_data_value(l, data_value(cv.total_value()));
    } else if (strcmp(field, "shards") == 0) {
        lua_createtable(l, 0, 0);
        int i = 1; // Lua arrays start from 1
        for (const auto& shard : cv.shards()) {
            lua_createtable(l, 0, 3);

            lua::push_sstring(l, shard.id().to_sstring());
            lua_setfield(l, -2, "id");

            lua_pushinteger(l, shard.value());
            lua_setfield(l, -2, "value");

            lua_pushinteger(l, shard.logical_clock());
            lua_setfield(l, -2, "clock");

            lua_seti(l, -2, i++);
        }
        return 1;
    }
    return 0;
}

struct atomic_cell_with_type {
    atomic_cell data;
    data_type type;

    atomic_cell_with_type(atomic_cell_view data_view, data_type type) : data(*type, data_view), type(std::move(type)) { }
};

int atomic_cell_index_l(lua_State* l) {
    auto field = lua_tostring(l, 2);
    auto& ct = pop_userdata_as_ref<atomic_cell_with_type>(l, 1);
    auto& cell = ct.data;
    auto& type = ct.type;
    lua_pop(l, 2);
    if (strcmp(field, "timestamp") == 0) {
        lua_pushinteger(l, cell.timestamp());
        return 1;
    } else if (strcmp(field, "type") == 0) {
        if (type->is_counter()) {
            if (cell.is_counter_update()) {
                lua_pushliteral(l, "counter-update");
            } else {
                lua_pushliteral(l, "counter-shards");
            }
        } else if (type->is_collection()) {
            if (type->is_atomic()) {
                lua_pushliteral(l, "frozen-collection");
            } else {
                lua_pushliteral(l, "collection");
            }
        } else {
            lua_pushliteral(l, "regular");
        }
        return 1;
    } else if (strcmp(field, "is_live") == 0) {
        lua_pushboolean(l, cell.is_live());
        return 1;
    } else if (strcmp(field, "has_ttl") == 0) {
        lua_pushboolean(l, cell.is_live_and_has_ttl());
        return 1;
    } else if (strcmp(field, "ttl") == 0) {
        if (!cell.is_live_and_has_ttl()) {
            return 0;
        }
        lua_pushinteger(l, std::chrono::duration_cast<std::chrono::seconds>(cell.ttl()).count());
        return 1;
    } else if (strcmp(field, "expiry") == 0) {
        if (!cell.is_live_and_has_ttl()) {
            return 0;
        }
        push_gc_clock_time_point(l, cell.expiry());
        return 1;
    } else if (strcmp(field, "deletion_time") == 0) {
        if (!cell.is_dead(gc_clock::now())) {
            return 0;
        }
        push_gc_clock_time_point(l, cell.deletion_time());
        return 1;
    } else if (strcmp(field, "value") == 0) {
        if (!cell.is_live()) {
            return 0;
        }
        if (type->is_counter()) {
            if (cell.is_counter_update()) {
                push_userdata<data_value>(l, cell.counter_update_value());
            } else {
                push_userdata<counter_shards_value>(l, counter_shards_value{type, managed_bytes(cell.serialize())});
            }
        } else {
            push_userdata<data_value>(l, type->deserialize(managed_bytes_view(cell.value())));
        }
        return 1;
    }
    return 0;
}

struct collection_with_type {
    collection_mutation data;
    data_type type;

    collection_with_type(collection_mutation_view data_view, data_type type) : data(*type, data_view), type(std::move(type)) { }
};

int collection_index_l(lua_State* l) {
    auto field = lua_tostring(l, 2);
    auto& ct = pop_userdata_as_ref<collection_with_type>(l, 1);
    auto type = ct.type;
    lua_pop(l, 2);
    return collection_mutation_view(ct.data).with_deserialized(*type, [l, field, type] (const collection_mutation_view_description& mv) {
        if (strcmp(field, "tombstone") == 0) {
            if (!mv.tomb) {
                return 0;
            }
            push_userdata<tombstone>(l, mv.tomb);
            return 1;
        } else if (strcmp(field, "type") == 0) {
            lua_pushliteral(l, "collection");
            return 1;
        } else if (strcmp(field, "values") == 0) {
            const auto size = mv.cells.size();
            std::function<void(size_t, bytes_view)> push_key;
            std::function<void(size_t, atomic_cell_view)> push_value;
            if (auto t = dynamic_cast<const collection_type_impl*>(type.get())) {
                push_key = [l, t = t->name_comparator()] (size_t, bytes_view k) {
                    push_userdata<data_value>(l, t->deserialize(k));
                };
                push_value = [l, t = t->value_comparator()] (size_t, atomic_cell_view v) {
                    push_userdata<atomic_cell_with_type>(l, v, t);
                };
            } else if (auto t = dynamic_cast<const tuple_type_impl*>(type.get())) {
                push_key = [l] (size_t, bytes_view) { lua_pushliteral(l, ""); };
                push_value = [l, t] (size_t i, atomic_cell_view v) {
                    push_userdata<atomic_cell_with_type>(l, v, t->type(i));
                };
            } else {
                return 0;
            }

            lua_createtable(l, size, 0);

            for (size_t i = 0; i < size; ++i) {
                const auto& e = mv.cells[i];

                lua_createtable(l, 0, 3);

                push_key(i, e.first);
                lua_setfield(l, -2, "key");

                push_value(i, e.second);
                lua_setfield(l, -2, "value");

                lua_seti(l, -2, i + 1); // Lua arrays start from 1
            }
            return 1;
        }
        return 0;
    });
}

int push_cells(lua_State* l, const row& cells, column_kind kind) {
    auto& schema = get_schema_l(l);
    lua_createtable(l, 0, 0);
    cells.for_each_cell([l, &schema, kind] (column_id id, const atomic_cell_or_collection& cell) {
        auto cdef = schema.column_at(kind, id);
        if (cdef.is_atomic()) {
            push_userdata<atomic_cell_with_type>(l, cell.as_atomic_cell(cdef), cdef.type);
        } else if (cdef.type->is_collection() || cdef.type->is_user_type()) {
            push_userdata<collection_with_type>(l, cell.as_collection_mutation(), cdef.type);
        } else {
            lua_pushnil(l);
        }
        lua_setfield(l, -2, cdef.name_as_text().data());
    });
    return 1;
}

int static_row_index_l(lua_State* l) {
    auto field = lua_tostring(l, 2);
    auto& sr = pop_userdata_as_ref<static_row>(l, 1);
    lua_pop(l, 2);
    if (strcmp(field, "cells") == 0) {
        return push_cells(l, sr.cells(), column_kind::static_column);
    }
    return 0;
}

int row_marker_index_l(lua_State* l) {
    auto field = lua_tostring(l, 2);
    auto& m = pop_userdata_as_ref<row_marker>(l, 1);
    lua_pop(l, 2);
    if (strcmp(field, "timestamp") == 0) {
        lua_pushinteger(l, m.timestamp());
        return 1;
    } else if (strcmp(field, "is_live") == 0) {
        lua_pushboolean(l, m.is_live());
        return 1;
    } else if (strcmp(field, "has_ttl") == 0) {
        lua_pushboolean(l, m.is_expiring());
        return 1;
    } else if (strcmp(field, "ttl") == 0) {
        if (!m.is_live() || !m.is_expiring()) {
            return 0;
        }
        lua_pushinteger(l, std::chrono::duration_cast<std::chrono::seconds>(m.ttl()).count());
        return 1;
    } else if (strcmp(field, "expiry") == 0) {
        if (!m.is_live() || !m.is_expiring()) {
            return 0;
        }
        push_gc_clock_time_point(l, m.expiry());
        return 1;
    } else if (strcmp(field, "deletion_time") == 0) {
        if (!m.is_expiring() && !m.is_dead(gc_clock::now())) {
            return 0;
        }
        push_gc_clock_time_point(l, m.deletion_time());
        return 1;
    }
    return 0;
}

int clustering_row_index_l(lua_State* l) {
    auto field = lua_tostring(l, 2);
    auto& cr = pop_userdata_as_ref<clustering_row>(l, 1);
    lua_pop(l, 2);
    if (strcmp(field, "key") == 0) {
        push_userdata<clustering_key>(l, cr.key());
        return 1;
    } else if (strcmp(field, "tombstone") == 0) {
        if (!cr.tomb()) {
            return 0;
        }
        push_userdata<tombstone>(l, cr.tomb().regular());
        return 1;
    } else if (strcmp(field, "shadowable_tombstone") == 0) {
        if (!cr.tomb()) {
            return 0;
        }
        push_userdata<tombstone>(l, cr.tomb().shadowable().tomb());
        return 1;
    } else if (strcmp(field, "marker") == 0) {
        if (cr.marker().is_missing()) {
            return 0;
        }
        push_userdata<row_marker>(l, cr.marker());
        return 1;
    } else if (strcmp(field, "cells") == 0) {
        return push_cells(l, cr.cells(), column_kind::regular_column);
    }
    return 0;
}

int range_tombstone_change_tostring_l(lua_State* l) {
    auto& rtc = pop_userdata_as_ref<range_tombstone_change>(l, 1);
    lua_pop(l, 1);
    lua::push_sstring(l, format("{}", rtc));
    return 1;
}

int range_tombstone_change_index_l(lua_State* l) {
    auto field = lua_tostring(l, 2);
    auto& rtc = pop_userdata_as_ref<range_tombstone_change>(l, 1);
    lua_pop(l, 2);
    const auto& pos = rtc.position();
    if (strcmp(field, "key") == 0) {
        if (!pos.has_key()) {
            return 0;
        }
        push_userdata<clustering_key>(l, pos.key());
        return 1;
    } else if (strcmp(field, "key_weight") == 0) {
        lua_pushinteger(l, static_cast<int>(pos.get_bound_weight()));
        return 1;
    } else if (strcmp(field, "tombstone") == 0) {
        if (!rtc.tombstone()) {
            return 0;
        }
        push_userdata<tombstone>(l, rtc.tombstone());
        return 1;
    } else if (strcmp(field, "__tostring") == 0) {
        lua_pushcfunction(l, range_tombstone_change_tostring_l);
        return 1;
    }
    return 0;
}

class json_writer {
    tools::mutation_fragment_stream_json_writer _writer;

private:
    static json_writer& get_this(lua_State* l) {
        return pop_userdata_as_ref<json_writer>(l, 1);
    }

    template <typename Function>
    static int do_invoke(lua_State* l, const char* name, Function&& fun) {
        try {
            fun(get_this(l));
        } catch (const std::exception& e) {
            return luaL_error(l, "json_writer::%s(): %s", name, e.what());
        }
        return 0;
    }

    template <typename Function>
    static int invoke(lua_State* l, const char* name, int expected_type, Function&& fun) {
        auto nargs = int(expected_type != LUA_TNIL) + 1;
        if (const auto n = lua_gettop(l); n != nargs) {
            return luaL_error(l, "json_writer::%s(): expected %I arguments, got %I", name, nargs, n);
        }
        const auto type = lua_type(l, 2);
        if (expected_type != LUA_TNIL && type != expected_type) {
            return luaL_error(l, "json_writer::%s(): expected argument of type %s, got %s", name, lua_typename(l, expected_type), lua_typename(l, type));
        }
        return do_invoke(l, name, fun);
    }

    template <typename Function>
    static int invoke_optional_arg(lua_State* l, const char* name, int expected_type, Function&& fun) {
        auto nargs = 2;
        const auto n = lua_gettop(l);
        if (n > nargs) {
            return luaL_error(l, "json_writer::%s(): expected at most %I arguments, got %I", name, nargs, n);
        }
        if (n == 2) {
            if (const auto type = lua_type(l, 2); type != LUA_TNIL && type != expected_type) {
                return luaL_error(l, "json_writer::%s(): expected argument of type %s, got %s", name, lua_typename(l, expected_type), lua_typename(l, type));
            }
        }
        return do_invoke(l, name, fun);
    }

public:
    json_writer(const schema& s) : _writer(s)
    { }
    static int null_l(lua_State* l) {
        return invoke(l, __FUNCTION__, LUA_TNIL, [] (json_writer& w) {
            w._writer.writer().Null();
        });
    }
    static int bool_l(lua_State* l) {
        return invoke(l, __FUNCTION__, LUA_TBOOLEAN, [l] (json_writer& w) {
            w._writer.writer().Bool(lua_toboolean(l, 2));
        });
    }
    static int int_l(lua_State* l) {
        return invoke(l, __FUNCTION__, LUA_TNUMBER, [l] (json_writer& w) {
            w._writer.writer().Int64(lua_tointeger(l, 2));
        });
    }
    static int double_l(lua_State* l) {
        return invoke(l, __FUNCTION__, LUA_TNUMBER, [l] (json_writer& w) {
            w._writer.writer().Double(lua_tonumber(l, 2));
        });
    }
    static int string_l(lua_State* l) {
        return invoke(l, __FUNCTION__, LUA_TSTRING, [l] (json_writer& w) {
            size_t len = 0;
            auto str = lua_tolstring(l, 2, &len);
            w._writer.writer().rjson_writer().String(str, len, false);
        });
    }
    static int start_object_l(lua_State* l) {
        return invoke(l, __FUNCTION__, LUA_TNIL, [] (json_writer& w) {
            w._writer.writer().StartObject();
        });
    }
    static int key_l(lua_State* l) {
        return invoke(l, __FUNCTION__, LUA_TSTRING, [l] (json_writer& w) {
            size_t len = 0;
            auto str = lua_tolstring(l, 2, &len);
            w._writer.writer().rjson_writer().Key(str, len, false);
        });
    }
    static int end_object_l(lua_State* l) {
        return invoke(l, __FUNCTION__, LUA_TNIL, [] (json_writer& w) {
            w._writer.writer().EndObject();
        });
    }
    static int start_array_l(lua_State* l) {
        return invoke(l, __FUNCTION__, LUA_TNIL, [] (json_writer& w) {
            w._writer.writer().StartArray();
        });
    }
    static int end_array_l(lua_State* l) {
        return invoke(l, __FUNCTION__, LUA_TNIL, [] (json_writer& w) {
            w._writer.writer().EndArray();
        });
    }
    static int start_stream_l(lua_State* l) {
        return invoke(l, __FUNCTION__, LUA_TNIL, [] (json_writer& w) {
            w._writer.start_stream();
        });
    }
    static int start_sstable_l(lua_State* l) {
        return invoke_optional_arg(l, __FUNCTION__, LUA_TUSERDATA, [l] (json_writer& w) {
            const sstables::sstable* sst = nullptr;
            if (lua_gettop(l) > 1 && lua_type(l, 2) != LUA_TNIL) {
                sst = &pop_userdata_ref<const sstables::sstable>(l, 2);
            }
            w._writer.start_sstable(sst);
        });
    }
    static int start_partition_l(lua_State* l) {
        return invoke(l, __FUNCTION__, LUA_TUSERDATA, [l] (json_writer& w) {
            w._writer.start_partition(pop_userdata_as_ref<const partition_start>(l, 2));
        });
    }
    static int static_row_l(lua_State* l) {
        return invoke(l, __FUNCTION__, LUA_TUSERDATA, [l] (json_writer& w) {
            w._writer.partition_element(pop_userdata_as_ref<const static_row>(l, 2));
        });
    }
    static int clustering_row_l(lua_State* l) {
        return invoke(l, __FUNCTION__, LUA_TUSERDATA, [l] (json_writer& w) {
            w._writer.partition_element(pop_userdata_as_ref<const clustering_row>(l, 2));
        });
    }
    static int range_tombstone_change_l(lua_State* l) {
        return invoke(l, __FUNCTION__, LUA_TUSERDATA, [l] (json_writer& w) {
            w._writer.partition_element(pop_userdata_as_ref<const range_tombstone_change>(l, 2));
        });
    }
    static int end_partition_l(lua_State* l) {
        return invoke(l, __FUNCTION__, LUA_TNIL, [] (json_writer& w) {
            w._writer.end_partition();
        });
    }
    static int end_sstable_l(lua_State* l) {
        return invoke(l, __FUNCTION__, LUA_TNIL, [] (json_writer& w) {
            w._writer.end_sstable();
        });
    }
    static int end_stream_l(lua_State* l) {
        return invoke(l, __FUNCTION__, LUA_TNIL, [] (json_writer& w) {
            w._writer.end_stream();
        });
    }
};

int json_writer_new_l(lua_State* l) {
    push_userdata<json_writer>(l, get_schema_l(l));
    return 1;
}

int json_writer_index_l(lua_State* l) {
    auto field = lua_tostring(l, 2);
    lua_pop(l, 2);
    std::pair<const char*, lua_CFunction> methods[] = {
            {"null", &json_writer::null_l},
            {"bool", &json_writer::bool_l},
            {"int", &json_writer::int_l},
            {"double", &json_writer::double_l},
            {"string", &json_writer::string_l},
            {"start_object", &json_writer::start_object_l},
            {"key", &json_writer::key_l},
            {"end_object", &json_writer::end_object_l},
            {"start_array", &json_writer::start_array_l},
            {"end_array", &json_writer::end_array_l},
            {"start_stream", &json_writer::start_stream_l},
            {"start_sstable", &json_writer::start_sstable_l},
            {"start_partition", &json_writer::start_partition_l},
            {"static_row", &json_writer::static_row_l},
            {"clustering_row", &json_writer::clustering_row_l},
            {"range_tombstone_change", &json_writer::range_tombstone_change_l},
            {"end_partition", &json_writer::end_partition_l},
            {"end_sstable", &json_writer::end_sstable_l},
            {"end_stream", &json_writer::end_stream_l},
    };
    for (const auto& [name, method] : methods) {
        if (strcmp(field, name) == 0) {
            lua_pushcfunction(l, method);
            return 1;
        }
    }
    return 0;
}

class lua_sstable_consumer : public sstable_consumer {
    schema_ptr _schema;
    reader_permit _permit;
    program_options::string_map _script_params;
    std::unique_ptr<lua_State, lua_closer> _l;
private:
    void register_metatables() {
        auto* l = _l.get();

        lua::register_metatables(l);

        luaL_newmetatable(l, get_metatable_name<column_definition>());
        lua_pushcfunction(l, column_definition_index_l);
        lua_setfield(l, -2, "__index");
        lua_pop(l, 1);

        luaL_newmetatable(l, get_metatable_name<schema>());
        lua_pushcfunction(l, schema_index_l);
        lua_setfield(l, -2, "__index");
        lua_pop(l, 1);

        luaL_newmetatable(l, get_metatable_name<partition_key>());
        lua_pushcfunction(l, key_index_l<partition_key>);
        lua_setfield(l, -2, "__index");
        lua_pushcclosure(l, tostring_with_wrapper<partition_key, partition_key::with_schema_wrapper>, 0);
        lua_setfield(l, -2, "__tostring");
        lua_pushcfunction(l, cxx_gc_l<partition_key>);
        lua_setfield(l, -2, "__gc");
        lua_pop(l, 1);

        luaL_newmetatable(l, get_metatable_name<clustering_key>());
        lua_pushcfunction(l, key_index_l<clustering_key>);
        lua_setfield(l, -2, "__index");
        lua_pushcclosure(l, tostring_with_wrapper<clustering_key, clustering_key::with_schema_wrapper>, 0);
        lua_setfield(l, -2, "__tostring");
        lua_pushcfunction(l, cxx_gc_l<clustering_key>);
        lua_setfield(l, -2, "__gc");
        lua_pop(l, 1);

        luaL_newmetatable(l, get_metatable_name<dht::ring_position_ext>());
        lua_pushcfunction(l, ring_position_index_l);
        lua_setfield(l, -2, "__index");
        lua_pushcfunction(l, tostring_l<dht::ring_position_ext>);
        lua_setfield(l, -2, "__tostring");
        lua_pushcfunction(l, cxx_gc_l<dht::ring_position_ext>);
        lua_setfield(l, -2, "__gc");
        lua_pop(l, 1);

        luaL_newmetatable(l, get_metatable_name<position_in_partition>());
        lua_pushcfunction(l, position_in_partition_index_l);
        lua_setfield(l, -2, "__index");
        lua_pushcfunction(l, tostring_l<position_in_partition>);
        lua_setfield(l, -2, "__tostring");
        lua_pushcfunction(l, cxx_gc_l<position_in_partition>);
        lua_setfield(l, -2, "__gc");
        lua_pop(l, 1);

        luaL_newmetatable(l, get_metatable_name<sstables::sstable>());
        lua_pushcfunction(l, sstable_index_l);
        lua_setfield(l, -2, "__index");
        lua_pop(l, 1);

        luaL_newmetatable(l, get_metatable_name<partition_start>());
        lua_pushcfunction(l, partition_start_index_l);
        lua_setfield(l, -2, "__index");
        lua_pushcfunction(l, tostring_l<partition_start>);
        lua_setfield(l, -2, "__tostring");
        lua_pushcfunction(l, cxx_gc_l<partition_start>);
        lua_setfield(l, -2, "__gc");
        lua_pop(l, 1);

        // type is used just to tag metatable, underlying type is a lua table
        luaL_newmetatable(l, get_metatable_name<gc_clock_time_point>());
        lua_pushcfunction(l, gc_clock_time_point_binary_op_l<std::equal_to<gc_clock::time_point>>);
        lua_setfield(l, -2, "__eq");
        lua_pushcfunction(l, gc_clock_time_point_binary_op_l<std::less<gc_clock::time_point>>);
        lua_setfield(l, -2, "__lt");
        lua_pushcfunction(l, gc_clock_time_point_binary_op_l<std::less_equal<gc_clock::time_point>>);
        lua_setfield(l, -2, "__le");
        lua_pushcfunction(l, gc_clock_time_point_tostring_l);
        lua_setfield(l, -2, "__tostring");
        lua_pop(l, 1);

        luaL_newmetatable(l, get_metatable_name<tombstone>());
        lua_pushcfunction(l, tombstone_index_l);
        lua_setfield(l, -2, "__index");
        lua_pushcfunction(l, tostring_l<tombstone>);
        lua_setfield(l, -2, "__tostring");
        lua_pushcfunction(l, cxx_gc_l<tombstone>);
        lua_setfield(l, -2, "__gc");
        lua_pop(l, 1);

        luaL_newmetatable(l, get_metatable_name<data_value>());
        lua_pushcfunction(l, data_value_index_l);
        lua_setfield(l, -2, "__index");
        lua_pushcfunction(l, data_value_tostring_l);
        lua_setfield(l, -2, "__tostring");
        lua_pushcfunction(l, cxx_gc_l<data_value>);
        lua_setfield(l, -2, "__gc");
        lua_pop(l, 1);

        luaL_newmetatable(l, get_metatable_name<counter_shards_value>());
        lua_pushcfunction(l, counter_shards_value_index_l);
        lua_setfield(l, -2, "__index");
        lua_pushcfunction(l, counter_shards_value_tostring_l);
        lua_setfield(l, -2, "__tostring");
        lua_pushcfunction(l, cxx_gc_l<counter_shards_value>);
        lua_setfield(l, -2, "__gc");
        lua_pop(l, 1);

        luaL_newmetatable(l, get_metatable_name<atomic_cell_with_type>());
        lua_pushcfunction(l, atomic_cell_index_l);
        lua_setfield(l, -2, "__index");
        lua_pushcfunction(l, cxx_gc_l<atomic_cell_with_type>);
        lua_setfield(l, -2, "__gc");
        lua_pop(l, 1);

        luaL_newmetatable(l, get_metatable_name<collection_with_type>());
        lua_pushcfunction(l, collection_index_l);
        lua_setfield(l, -2, "__index");
        lua_pushcfunction(l, cxx_gc_l<collection_with_type>);
        lua_setfield(l, -2, "__gc");
        lua_pop(l, 1);

        luaL_newmetatable(l, get_metatable_name<static_row>());
        lua_pushcfunction(l, static_row_index_l);
        lua_setfield(l, -2, "__index");
        lua_pushcclosure(l, tostring_with_wrapper<static_row, static_row::printer>, 0);
        lua_setfield(l, -2, "__tostring");
        lua_pushcfunction(l, cxx_gc_l<static_row>);
        lua_setfield(l, -2, "__gc");
        lua_pop(l, 1);

        luaL_newmetatable(l, get_metatable_name<clustering_row>());
        lua_pushcfunction(l, clustering_row_index_l);
        lua_setfield(l, -2, "__index");
        lua_pushcclosure(l, tostring_with_wrapper<clustering_row, clustering_row::printer>, 0);
        lua_setfield(l, -2, "__tostring");
        lua_pushcfunction(l, cxx_gc_l<clustering_row>);
        lua_setfield(l, -2, "__gc");
        lua_pop(l, 1);

        luaL_newmetatable(l, get_metatable_name<row_marker>());
        lua_pushcfunction(l, row_marker_index_l);
        lua_setfield(l, -2, "__index");
        lua_pushcfunction(l, cxx_gc_l<row_marker>);
        lua_setfield(l, -2, "__gc");
        lua_pop(l, 1);

        luaL_newmetatable(l, get_metatable_name<range_tombstone_change>());
        lua_pushcfunction(l, range_tombstone_change_index_l);
        lua_setfield(l, -2, "__index");
        lua_pushcfunction(l, cxx_gc_l<range_tombstone_change>);
        lua_setfield(l, -2, "__gc");
        lua_pop(l, 1);

        luaL_newmetatable(l, get_metatable_name<json_writer>());
        lua_pushcfunction(l, json_writer_index_l);
        lua_setfield(l, -2, "__index");
        lua_pushcfunction(l, cxx_gc_l<json_writer>);
        lua_setfield(l, -2, "__gc");
        lua_pop(l, 1);
    }

    void register_types() {
        auto* l = _l.get();
        const luaL_Reg scylla_lib[] = {
            {"now", gc_clock_time_point_now_l},
            {"time_point_from_string", gc_clock_time_point_from_string_l},
            {"new_json_writer", json_writer_new_l},
            {"new_ring_position", new_ring_position_l},
            {"new_position_in_partition", new_position_in_partition_l},
            {"unserialize_partition_key", unserialize_key_l<allow_prefixes::no>},
            {"unserialize_clustering_key", unserialize_key_l<allow_prefixes::yes>},
            {"token_of", token_of_l},
            {NULL, NULL},
        };
        luaL_newlib(l, scylla_lib);
        lua_setglobal(l, "Scylla");
    }

    void register_globals() {
        auto* l = _l.get();

        push_userdata_ref<const schema>(l, *_schema.get());
        lua_setglobal(l, "schema");
    }

    future<int> call(std::string_view func_name, int nargs) {
        auto* l = _l.get();
        int ret = LUA_YIELD;
        int nresults = 0;
        while (ret == LUA_YIELD) {
            ret = lua_resume(l, nullptr, nargs, &nresults);
            if (ret == LUA_YIELD) {
                if (nresults == 0) {
                    co_await coroutine::maybe_yield();
                } else if (nresults != 1) {
                    throw std::runtime_error(fmt::format("{} failed: unexpected number of results yielded, expected 1, got {}", func_name, nresults));
                } else {
                    if (!lua_isuserdata(l, -1)) {
                        throw std::runtime_error(fmt::format("{} failed: only future<> is allowed to be yielded from a coroutine", func_name));
                    }
                    co_await pop_userdata<future<>>(l, -1);
                    lua_pop(l, 1);
                }
            }
        }
        if (ret != LUA_OK) {
            throw std::runtime_error(fmt::format("{} failed: {}", func_name, lua_tostring(l, -1)));
        }
        co_return nresults;
    }

    struct invoke_meta {
        int nargs;
        bool returns_stop_iteration;
    };
    template <typename Func>
    future<stop_iteration> invoke_script_method(const char* name, Func&& prepare_stack) {
        auto* l = _l.get();
        // basic stack sanity check
        if (auto n = lua_gettop(l); n) {
            throw std::runtime_error(fmt::format("{}() failed: stack expected to be empty (clean), but got {} items", name, n));
        }
        const auto type = lua_getglobal(l, name);
        // check whether script has this method, short-circuit if not
        if (type == LUA_TNIL) {
            lua_pop(l, 1);
            co_return stop_iteration::no;
        }
        // is symbol callable?
        if (type != LUA_TFUNCTION) {
            throw std::runtime_error(fmt::format("{}() failed: symbol is not callable", name));
        }
        const auto desc = prepare_stack();
        // sanity check for argument count
        // function to be called is also on the stack, account for that
        if (auto n = lua_gettop(l); n != desc.nargs + 1) {
            throw std::runtime_error(fmt::format("{}() failed: unexpected number of arguments for function, expected: {}, got: {}", name, desc.nargs, n - 1));
        }
        // call + sanity check for return value count
        const auto nret = co_await call(name, desc.nargs);
        if (nret > int(desc.returns_stop_iteration)) {
            throw std::runtime_error(fmt::format("{}() failed: unexpected number of return values from function, expected: {}, got: {}", name, int(desc.returns_stop_iteration), nret));
        }
        // extract and convert return value if method has it (it can only be stop_iteration)
        // we allow method to return nothing, int this case we assume stop_iteration::no
        auto stop = stop_iteration::no;
        if (desc.returns_stop_iteration && nret) {
            // lua method return false when they don't want to continue
            // stop_iteration expressed in boolean is too counter-intuitive
            stop = stop_iteration(!lua_toboolean(l, 1));
            lua_pop(l, 1);
        }
        co_return stop;
    }

public:
    explicit lua_sstable_consumer(schema_ptr s, reader_permit p, program_options::string_map script_params)
        : _schema(std::move(s))
        , _permit(std::move(p))
        , _script_params(std::move(script_params))
        , _l(lua_newstate(lua_alloc, nullptr))
    {
    }
    future<> load_script(std::string_view script_name, std::string_view script) {
        auto* l = _l.get();
        // load lua standard libraries
        for (const luaL_Reg* lib = loadedlibs; lib->func; lib++) {
            luaL_requiref(l, lib->name, lib->func, 1);
            lua_pop(l, 1);
        }
        register_metatables();
        register_types();
        register_globals();
        // parse and load script
        if (luaL_loadbuffer(l, script.data(), script.size(), script_name.data())) {
            throw std::runtime_error(fmt::format("Failed to load {}: {}", script_name, lua_tostring(l, -1)));
        }
        // execute script (runs global code)
        if (auto n = co_await call(script_name, 0); n) {
            throw std::runtime_error(fmt::format("Failed to execute {}: unexpected return value, expected 0, got {}", script_name, n));
        }
        co_return;
    }
    virtual future<> consume_stream_start() override {
        return invoke_script_method(__FUNCTION__, [this] {
            auto* l = _l.get();
            lua_newtable(l);
            for (const auto& [k, v] : _script_params) {
                lua::push_sstring(l, v);
                lua_setfield(l, -2, k.data());
            }
            return invoke_meta{1, false};
        }).discard_result();
    }
    virtual future<stop_iteration> consume_sstable_start(const sstables::sstable* const sst) override {
        return invoke_script_method(__FUNCTION__, [this, sst] {
            auto* l = _l.get();
            if (sst) {
                push_userdata_ref<const sstables::sstable>(l, *sst);
            } else {
                lua_pushnil(l);
            }
            return invoke_meta{1, true};
        });
    }
    virtual future<stop_iteration> consume(partition_start&& ps) override {
        return invoke_script_method("consume_partition_start", [this, ps = std::move(ps)] () mutable {
            auto* l = _l.get();
            push_userdata<partition_start>(l, std::move(ps));
            return invoke_meta{1, true};
        });
    }
    virtual future<stop_iteration> consume(static_row&& sr) override {
        return invoke_script_method("consume_static_row", [this, sr = std::move(sr)] () mutable {
            auto* l = _l.get();
            push_userdata<static_row>(l, std::move(sr));
            return invoke_meta{1, true};
        });
    }
    virtual future<stop_iteration> consume(clustering_row&& cr) override {
        return invoke_script_method("consume_clustering_row", [this, cr = std::move(cr)] () mutable {
            auto* l = _l.get();
            push_userdata<clustering_row>(l, std::move(cr));
            return invoke_meta{1, true};
        });
    }
    virtual future<stop_iteration> consume(range_tombstone_change&& rtc) override {
        return invoke_script_method("consume_range_tombstone_change", [this, rtc = std::move(rtc)] () mutable {
            auto* l = _l.get();
            push_userdata<range_tombstone_change>(l, std::move(rtc));
            return invoke_meta{1, true};
        });
    }
    virtual future<stop_iteration> consume(partition_end&& pe) override {
        return invoke_script_method("consume_partition_end", [] {
            return invoke_meta{0, true};
        });
    }
    virtual future<stop_iteration> consume_sstable_end() override {
        return invoke_script_method(__FUNCTION__, [] {
            return invoke_meta{0, true};
        });
    }
    virtual future<> consume_stream_end() override {
        return invoke_script_method(__FUNCTION__, [] {
            return invoke_meta{0, false};
        }).discard_result();
    }
};

}

future<std::unique_ptr<sstable_consumer>> make_lua_sstable_consumer(schema_ptr s, reader_permit p, std::string_view script_path,
        program_options::string_map script_args) {
    auto consumer = std::make_unique<lua_sstable_consumer>(std::move(s), std::move(p), std::move(script_args));

    auto file = co_await open_file_dma(script_path, open_flags::ro);
    auto fstream = make_file_input_stream(file);
    auto script = co_await util::read_entire_stream_contiguous(fstream);
    co_await consumer->load_script(script_path, script);

    co_return consumer;
}
