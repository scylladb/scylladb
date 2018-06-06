/*
 * Copyright (C) 2014 ScyllaDB
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

#include <iostream>
#include <iterator>
#include <regex>

#include "cql3_type.hh"
#include "cql3/util.hh"
#include "ut_name.hh"

namespace cql3 {

sstring cql3_type::to_string() const {
    if (_type->is_user_type()) {
        return "frozen<" + util::maybe_quote(_name) + ">";
    }
    if (_type->is_tuple()) {
        return "frozen<" + _name + ">";
    }
    return _name;
}

shared_ptr<cql3_type> cql3_type::raw::prepare(database& db, const sstring& keyspace) {
    try {
        auto&& ks = db.find_keyspace(keyspace);
        return prepare_internal(keyspace, ks.metadata()->user_types());
    } catch (no_such_keyspace& nsk) {
        throw exceptions::invalid_request_exception("Unknown keyspace " + keyspace);
    }
}

bool cql3_type::raw::is_duration() const {
    return false;
}

bool cql3_type::raw::references_user_type(const sstring& name) const {
    return false;
}

class cql3_type::raw_type : public raw {
private:
    shared_ptr<cql3_type> _type;
public:
    raw_type(shared_ptr<cql3_type> type)
        : _type{type}
    { }
public:
    virtual shared_ptr<cql3_type> prepare(database& db, const sstring& keyspace) {
        return _type;
    }
    shared_ptr<cql3_type> prepare_internal(const sstring&, lw_shared_ptr<user_types_metadata>) override {
        return _type;
    }

    virtual bool supports_freezing() const {
        return false;
    }

    virtual bool is_counter() const {
        return _type->is_counter();
    }

    virtual sstring to_string() const {
        return _type->to_string();
    }

    virtual bool is_duration() const override {
        return _type->get_type()->equals(duration_type);
    }
};

class cql3_type::raw_collection : public raw {
    const collection_type_impl::kind* _kind;
    shared_ptr<raw> _keys;
    shared_ptr<raw> _values;
public:
    raw_collection(const collection_type_impl::kind* kind, shared_ptr<raw> keys, shared_ptr<raw> values)
            : _kind(kind), _keys(std::move(keys)), _values(std::move(values)) {
    }

    virtual void freeze() override {
        if (_keys && _keys->supports_freezing()) {
            _keys->freeze();
        }
        if (_values && _values->supports_freezing()) {
            _values->freeze();
        }
        _frozen = true;
    }

    virtual bool supports_freezing() const override {
        return true;
    }

    virtual bool is_collection() const override {
        return true;
    }

    virtual shared_ptr<cql3_type> prepare_internal(const sstring& keyspace, lw_shared_ptr<user_types_metadata> user_types) override {
        assert(_values); // "Got null values type for a collection";

        if (!_frozen && _values->supports_freezing() && !_values->_frozen) {
            throw exceptions::invalid_request_exception(sprint("Non-frozen collections are not allowed inside collections: %s", *this));
        }
        if (_values->is_counter()) {
            throw exceptions::invalid_request_exception(sprint("Counters are not allowed inside collections: %s", *this));
        }

        if (_keys) {
            if (!_frozen && _keys->supports_freezing() && !_keys->_frozen) {
                throw exceptions::invalid_request_exception(sprint("Non-frozen collections are not allowed inside collections: %s", *this));
            }
        }

        if (_kind == &collection_type_impl::kind::list) {
            return make_shared(cql3_type(to_string(), list_type_impl::get_instance(_values->prepare_internal(keyspace, user_types)->get_type(), !_frozen), false));
        } else if (_kind == &collection_type_impl::kind::set) {
            if (_values->is_duration()) {
                throw exceptions::invalid_request_exception(sprint("Durations are not allowed inside sets: %s", *this));
            }
            return make_shared(cql3_type(to_string(), set_type_impl::get_instance(_values->prepare_internal(keyspace, user_types)->get_type(), !_frozen), false));
        } else if (_kind == &collection_type_impl::kind::map) {
            assert(_keys); // "Got null keys type for a collection";
            if (_keys->is_duration()) {
                throw exceptions::invalid_request_exception(sprint("Durations are not allowed as map keys: %s", *this));
            }
            return make_shared(cql3_type(to_string(), map_type_impl::get_instance(_keys->prepare_internal(keyspace, user_types)->get_type(), _values->prepare_internal(keyspace, user_types)->get_type(), !_frozen), false));
        }
        abort();
    }

    bool references_user_type(const sstring& name) const override {
        return (_keys && _keys->references_user_type(name)) || _values->references_user_type(name);
    }

    bool is_duration() const override {
        return false;
    }

    virtual sstring to_string() const override {
        sstring start = _frozen ? "frozen<" : "";
        sstring end = _frozen ? ">" : "";
        if (_kind == &collection_type_impl::kind::list) {
            return sprint("%slist<%s>%s", start, _values, end);
        } else if (_kind == &collection_type_impl::kind::set) {
            return sprint("%sset<%s>%s", start, _values, end);
        } else if (_kind == &collection_type_impl::kind::map) {
            return sprint("%smap<%s, %s>%s", start, _keys, _values, end);
        }
        abort();
    }
};

class cql3_type::raw_ut : public raw {
    ut_name _name;
public:
    raw_ut(ut_name name)
            : _name(std::move(name)) {
    }

    virtual std::experimental::optional<sstring> keyspace() const override {
        return _name.get_keyspace();
    }

    virtual void freeze() override {
        _frozen = true;
    }

    virtual shared_ptr<cql3_type> prepare_internal(const sstring& keyspace, lw_shared_ptr<user_types_metadata> user_types) override {
        if (_name.has_keyspace()) {
            // The provided keyspace is the one of the current statement this is part of. If it's different from the keyspace of
            // the UTName, we reject since we want to limit user types to their own keyspace (see #6643)
            if (!keyspace.empty() && keyspace != _name.get_keyspace()) {
                throw exceptions::invalid_request_exception(sprint("Statement on keyspace %s cannot refer to a user type in keyspace %s; "
                                                                   "user types can only be used in the keyspace they are defined in",
                                                                keyspace, _name.get_keyspace()));
            }
        } else {
            _name.set_keyspace(keyspace);
        }
        if (!user_types) {
            // bootstrap mode.
            throw exceptions::invalid_request_exception(sprint("Unknown type %s", _name));
        }
        try {
            auto&& type = user_types->get_type(_name.get_user_type_name());
            if (!_frozen) {
                throw exceptions::invalid_request_exception("Non-frozen User-Defined types are not supported, please use frozen<>");
            }
            return make_shared<cql3_type>(_name.to_string(), std::move(type));
        } catch (std::out_of_range& e) {
            throw exceptions::invalid_request_exception(sprint("Unknown type %s", _name));
        }
    }
    bool references_user_type(const sstring& name) const override {
        return _name.get_string_type_name() == name;
    }
    virtual bool supports_freezing() const override {
        return true;
    }

    virtual sstring to_string() const override {
        return _name.to_string();
    }
};


class cql3_type::raw_tuple : public raw {
    std::vector<shared_ptr<raw>> _types;
public:
    raw_tuple(std::vector<shared_ptr<raw>> types)
            : _types(std::move(types)) {
    }
    virtual bool supports_freezing() const override {
        return true;
    }
    virtual bool is_collection() const override {
        return false;
    }
    virtual void freeze() override {
        for (auto&& t : _types) {
            if (t->supports_freezing()) {
                t->freeze();
            }
        }
        _frozen = true;
    }
    virtual shared_ptr<cql3_type> prepare_internal(const sstring& keyspace, lw_shared_ptr<user_types_metadata> user_types) override {
        if (!_frozen) {
            freeze();
        }
        std::vector<data_type> ts;
        for (auto&& t : _types) {
            if (t->is_counter()) {
                throw exceptions::invalid_request_exception("Counters are not allowed inside tuples");
            }
            ts.push_back(t->prepare_internal(keyspace, user_types)->get_type());
        }
        return make_cql3_tuple_type(tuple_type_impl::get_instance(std::move(ts)));
    }

    bool references_user_type(const sstring& name) const override {
        return std::any_of(_types.begin(), _types.end(), [&name](auto t) {
            return t->references_user_type(name);
        });
    }

    virtual sstring to_string() const override {
        return sprint("tuple<%s>", join(", ", _types));
    }
};

bool
cql3_type::raw::is_collection() const {
    return false;
}

bool
cql3_type::raw::is_counter() const {
    return false;
}

std::experimental::optional<sstring>
cql3_type::raw::keyspace() const {
    return std::experimental::nullopt;
}

void
cql3_type::raw::freeze() {
    sstring message = sprint("frozen<> is only allowed on collections, tuples, and user-defined types (got %s)", to_string());
    throw exceptions::invalid_request_exception(message);
}

shared_ptr<cql3_type::raw>
cql3_type::raw::from(shared_ptr<cql3_type> type) {
    return ::make_shared<raw_type>(type);
}

shared_ptr<cql3_type::raw>
cql3_type::raw::user_type(ut_name name) {
    return ::make_shared<raw_ut>(name);
}

shared_ptr<cql3_type::raw>
cql3_type::raw::map(shared_ptr<raw> t1, shared_ptr<raw> t2) {
    return make_shared(raw_collection(&collection_type_impl::kind::map, std::move(t1), std::move(t2)));
}

shared_ptr<cql3_type::raw>
cql3_type::raw::list(shared_ptr<raw> t) {
    return make_shared(raw_collection(&collection_type_impl::kind::list, {}, std::move(t)));
}

shared_ptr<cql3_type::raw>
cql3_type::raw::set(shared_ptr<raw> t) {
    return make_shared(raw_collection(&collection_type_impl::kind::set, {}, std::move(t)));
}

shared_ptr<cql3_type::raw>
cql3_type::raw::tuple(std::vector<shared_ptr<raw>> ts) {
    return make_shared(raw_tuple(std::move(ts)));
}

shared_ptr<cql3_type::raw>
cql3_type::raw::frozen(shared_ptr<raw> t) {
    t->freeze();
    return t;
}

thread_local shared_ptr<cql3_type> cql3_type::ascii = make("ascii", ascii_type, cql3_type::kind::ASCII);
thread_local shared_ptr<cql3_type> cql3_type::bigint = make("bigint", long_type, cql3_type::kind::BIGINT);
thread_local shared_ptr<cql3_type> cql3_type::blob = make("blob", bytes_type, cql3_type::kind::BLOB);
thread_local shared_ptr<cql3_type> cql3_type::boolean = make("boolean", boolean_type, cql3_type::kind::BOOLEAN);
thread_local shared_ptr<cql3_type> cql3_type::double_ = make("double", double_type, cql3_type::kind::DOUBLE);
thread_local shared_ptr<cql3_type> cql3_type::empty = make("empty", empty_type, cql3_type::kind::EMPTY);
thread_local shared_ptr<cql3_type> cql3_type::float_ = make("float", float_type, cql3_type::kind::FLOAT);
thread_local shared_ptr<cql3_type> cql3_type::int_ = make("int", int32_type, cql3_type::kind::INT);
thread_local shared_ptr<cql3_type> cql3_type::smallint = make("smallint", short_type, cql3_type::kind::SMALLINT);
thread_local shared_ptr<cql3_type> cql3_type::text = make("text", utf8_type, cql3_type::kind::TEXT);
thread_local shared_ptr<cql3_type> cql3_type::timestamp = make("timestamp", timestamp_type, cql3_type::kind::TIMESTAMP);
thread_local shared_ptr<cql3_type> cql3_type::tinyint = make("tinyint", byte_type, cql3_type::kind::TINYINT);
thread_local shared_ptr<cql3_type> cql3_type::uuid = make("uuid", uuid_type, cql3_type::kind::UUID);
thread_local shared_ptr<cql3_type> cql3_type::varchar = make("varchar", utf8_type, cql3_type::kind::TEXT);
thread_local shared_ptr<cql3_type> cql3_type::timeuuid = make("timeuuid", timeuuid_type, cql3_type::kind::TIMEUUID);
thread_local shared_ptr<cql3_type> cql3_type::date = make("date", simple_date_type, cql3_type::kind::DATE);
thread_local shared_ptr<cql3_type> cql3_type::time = make("time", time_type, cql3_type::kind::TIME);
thread_local shared_ptr<cql3_type> cql3_type::inet = make("inet", inet_addr_type, cql3_type::kind::INET);
thread_local shared_ptr<cql3_type> cql3_type::varint = make("varint", varint_type, cql3_type::kind::VARINT);
thread_local shared_ptr<cql3_type> cql3_type::decimal = make("decimal", decimal_type, cql3_type::kind::DECIMAL);
thread_local shared_ptr<cql3_type> cql3_type::counter = make("counter", counter_type, cql3_type::kind::COUNTER);
thread_local shared_ptr<cql3_type> cql3_type::duration = make("duration", duration_type, cql3_type::kind::DURATION);

const std::vector<shared_ptr<cql3_type>>&
cql3_type::values() {
    static thread_local std::vector<shared_ptr<cql3_type>> v = {
        cql3_type::ascii,
        cql3_type::bigint,
        cql3_type::blob,
        cql3_type::boolean,
        cql3_type::counter,
        cql3_type::decimal,
        cql3_type::double_,
        cql3_type::empty,
        cql3_type::float_,
        cql3_type::inet,
        cql3_type::int_,
        cql3_type::smallint,
        cql3_type::text,
        cql3_type::timestamp,
        cql3_type::tinyint,
        cql3_type::uuid,
        cql3_type::varchar,
        cql3_type::varint,
        cql3_type::timeuuid,
        cql3_type::date,
        cql3_type::time,
        cql3_type::duration,
    };
    return v;
}

shared_ptr<cql3_type>
make_cql3_tuple_type(tuple_type t) {
    auto name = sprint("tuple<%s>",
                       ::join(", ",
                            t->all_types()
                            | boost::adaptors::transformed(std::mem_fn(&abstract_type::as_cql3_type))));
    return ::make_shared<cql3_type>(std::move(name), std::move(t), false);
}


std::ostream&
operator<<(std::ostream& os, const cql3_type::raw& r) {
    return os << r.to_string();
}

namespace util {

sstring maybe_quote(const sstring& identifier) {
    static const std::regex unquoted_identifier_re("[a-z][a-z0-9_]*");
    if (std::regex_match(identifier.begin(), identifier.end(), unquoted_identifier_re)) {
        return identifier;
    }
    static const std::regex double_quote_re("\"");
    std::string result = identifier;
    std::regex_replace(result, double_quote_re, "\"\"");
    return '"' + result + '"';
}

}

}

