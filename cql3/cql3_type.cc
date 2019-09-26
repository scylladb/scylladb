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
#include "database.hh"
#include "user_types_metadata.hh"
#include "types/map.hh"
#include "types/set.hh"
#include "types/list.hh"

namespace cql3 {

cql3_type cql3_type::raw::prepare(database& db, const sstring& keyspace) {
    try {
        auto&& ks = db.find_keyspace(keyspace);
        return prepare_internal(keyspace, *ks.metadata()->user_types());
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
    cql3_type _type;
public:
    raw_type(cql3_type type)
        : _type{type}
    { }
public:
    virtual cql3_type prepare(database& db, const sstring& keyspace) {
        return _type;
    }
    cql3_type prepare_internal(const sstring&, user_types_metadata&) override {
        return _type;
    }

    virtual bool supports_freezing() const {
        return false;
    }

    virtual bool is_counter() const {
        return _type.is_counter();
    }

    virtual sstring to_string() const {
        return _type.to_string();
    }

    virtual bool is_duration() const override {
        return _type.get_type() == duration_type;
    }
};

class cql3_type::raw_collection : public raw {
    const abstract_type::kind _kind;
    shared_ptr<raw> _keys;
    shared_ptr<raw> _values;
public:
    raw_collection(const abstract_type::kind kind, shared_ptr<raw> keys, shared_ptr<raw> values)
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

    virtual cql3_type prepare_internal(const sstring& keyspace, user_types_metadata& user_types) override {
        assert(_values); // "Got null values type for a collection";

        if (!_frozen && _values->supports_freezing() && !_values->_frozen) {
            throw exceptions::invalid_request_exception(format("Non-frozen collections are not allowed inside collections: {}", *this));
        }
        if (_values->is_counter()) {
            throw exceptions::invalid_request_exception(format("Counters are not allowed inside collections: {}", *this));
        }

        if (_keys) {
            if (!_frozen && _keys->supports_freezing() && !_keys->_frozen) {
                throw exceptions::invalid_request_exception(format("Non-frozen collections are not allowed inside collections: {}", *this));
            }
        }

        if (_kind == abstract_type::kind::list) {
            return cql3_type(list_type_impl::get_instance(_values->prepare_internal(keyspace, user_types).get_type(), !_frozen));
        } else if (_kind == abstract_type::kind::set) {
            if (_values->is_duration()) {
                throw exceptions::invalid_request_exception(format("Durations are not allowed inside sets: {}", *this));
            }
            return cql3_type(set_type_impl::get_instance(_values->prepare_internal(keyspace, user_types).get_type(), !_frozen));
        } else if (_kind == abstract_type::kind::map) {
            assert(_keys); // "Got null keys type for a collection";
            if (_keys->is_duration()) {
                throw exceptions::invalid_request_exception(format("Durations are not allowed as map keys: {}", *this));
            }
            return cql3_type(map_type_impl::get_instance(_keys->prepare_internal(keyspace, user_types).get_type(), _values->prepare_internal(keyspace, user_types).get_type(), !_frozen));
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
        if (_kind == abstract_type::kind::list) {
            return format("{}list<{}>{}", start, _values, end);
        } else if (_kind == abstract_type::kind::set) {
            return format("{}set<{}>{}", start, _values, end);
        } else if (_kind == abstract_type::kind::map) {
            return format("{}map<{}, {}>{}", start, _keys, _values, end);
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

    virtual std::optional<sstring> keyspace() const override {
        return _name.get_keyspace();
    }

    virtual void freeze() override {
        _frozen = true;
    }

    virtual cql3_type prepare_internal(const sstring& keyspace, user_types_metadata& user_types) override {
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
        try {
            auto&& type = user_types.get_type(_name.get_user_type_name());
            if (!_frozen) {
                throw exceptions::invalid_request_exception("Non-frozen User-Defined types are not supported, please use frozen<>");
            }
            return cql3_type(std::move(type));
        } catch (std::out_of_range& e) {
            throw exceptions::invalid_request_exception(format("Unknown type {}", _name));
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
    virtual cql3_type prepare_internal(const sstring& keyspace, user_types_metadata& user_types) override {
        if (!_frozen) {
            freeze();
        }
        std::vector<data_type> ts;
        for (auto&& t : _types) {
            if (t->is_counter()) {
                throw exceptions::invalid_request_exception("Counters are not allowed inside tuples");
            }
            ts.push_back(t->prepare_internal(keyspace, user_types).get_type());
        }
        return tuple_type_impl::get_instance(std::move(ts))->as_cql3_type();
    }

    bool references_user_type(const sstring& name) const override {
        return std::any_of(_types.begin(), _types.end(), [&name](auto t) {
            return t->references_user_type(name);
        });
    }

    virtual sstring to_string() const override {
        return format("tuple<{}>", join(", ", _types));
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

std::optional<sstring>
cql3_type::raw::keyspace() const {
    return std::nullopt;
}

void
cql3_type::raw::freeze() {
    sstring message = format("frozen<> is only allowed on collections, tuples, and user-defined types (got {})", to_string());
    throw exceptions::invalid_request_exception(message);
}

shared_ptr<cql3_type::raw>
cql3_type::raw::from(cql3_type type) {
    return ::make_shared<raw_type>(type);
}

shared_ptr<cql3_type::raw>
cql3_type::raw::user_type(ut_name name) {
    return ::make_shared<raw_ut>(name);
}

shared_ptr<cql3_type::raw>
cql3_type::raw::map(shared_ptr<raw> t1, shared_ptr<raw> t2) {
    return make_shared(raw_collection(abstract_type::kind::map, std::move(t1), std::move(t2)));
}

shared_ptr<cql3_type::raw>
cql3_type::raw::list(shared_ptr<raw> t) {
    return make_shared(raw_collection(abstract_type::kind::list, {}, std::move(t)));
}

shared_ptr<cql3_type::raw>
cql3_type::raw::set(shared_ptr<raw> t) {
    return make_shared(raw_collection(abstract_type::kind::set, {}, std::move(t)));
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

thread_local cql3_type cql3_type::ascii{ascii_type};
thread_local cql3_type cql3_type::bigint{long_type};
thread_local cql3_type cql3_type::blob{bytes_type};
thread_local cql3_type cql3_type::boolean{boolean_type};
thread_local cql3_type cql3_type::double_{double_type};
thread_local cql3_type cql3_type::empty{empty_type};
thread_local cql3_type cql3_type::float_{float_type};
thread_local cql3_type cql3_type::int_{int32_type};
thread_local cql3_type cql3_type::smallint{short_type};
thread_local cql3_type cql3_type::text{utf8_type};
thread_local cql3_type cql3_type::timestamp{timestamp_type};
thread_local cql3_type cql3_type::tinyint{byte_type};
thread_local cql3_type cql3_type::uuid{uuid_type};
thread_local cql3_type cql3_type::timeuuid{timeuuid_type};
thread_local cql3_type cql3_type::date{simple_date_type};
thread_local cql3_type cql3_type::time{time_type};
thread_local cql3_type cql3_type::inet{inet_addr_type};
thread_local cql3_type cql3_type::varint{varint_type};
thread_local cql3_type cql3_type::decimal{decimal_type};
thread_local cql3_type cql3_type::counter{counter_type};
thread_local cql3_type cql3_type::duration{duration_type};

const std::vector<cql3_type>&
cql3_type::values() {
    static thread_local std::vector<cql3_type> v = {
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
        cql3_type::varint,
        cql3_type::timeuuid,
        cql3_type::date,
        cql3_type::time,
        cql3_type::duration,
    };
    return v;
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

