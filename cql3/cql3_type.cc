/*
 * Copyright (C) 2014-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "utils/assert.hh"
#include <iterator>
#include <boost/regex.hpp>

#include "cql3_type.hh"
#include "cql3/util.hh"
#include "exceptions/exceptions.hh"
#include "ut_name.hh"
#include "data_dictionary/data_dictionary.hh"
#include "data_dictionary/user_types_metadata.hh"
#include "data_dictionary/keyspace_metadata.hh"
#include "types/map.hh"
#include "types/set.hh"
#include "types/list.hh"
#include "concrete_types.hh"

namespace cql3 {

static cql3_type::kind get_cql3_kind(const abstract_type& t) {
    struct visitor {
        cql3_type::kind operator()(const ascii_type_impl&) { return cql3_type::kind::ASCII; }
        cql3_type::kind operator()(const byte_type_impl&) { return cql3_type::kind::TINYINT; }
        cql3_type::kind operator()(const bytes_type_impl&) { return cql3_type::kind::BLOB; }
        cql3_type::kind operator()(const boolean_type_impl&) { return cql3_type::kind::BOOLEAN; }
        cql3_type::kind operator()(const counter_type_impl&) { return cql3_type::kind::COUNTER; }
        cql3_type::kind operator()(const decimal_type_impl&) { return cql3_type::kind::DECIMAL; }
        cql3_type::kind operator()(const double_type_impl&) { return cql3_type::kind::DOUBLE; }
        cql3_type::kind operator()(const duration_type_impl&) { return cql3_type::kind::DURATION; }
        cql3_type::kind operator()(const empty_type_impl&) { return cql3_type::kind::EMPTY; }
        cql3_type::kind operator()(const float_type_impl&) { return cql3_type::kind::FLOAT; }
        cql3_type::kind operator()(const inet_addr_type_impl&) { return cql3_type::kind::INET; }
        cql3_type::kind operator()(const int32_type_impl&) { return cql3_type::kind::INT; }
        cql3_type::kind operator()(const long_type_impl&) { return cql3_type::kind::BIGINT; }
        cql3_type::kind operator()(const short_type_impl&) { return cql3_type::kind::SMALLINT; }
        cql3_type::kind operator()(const simple_date_type_impl&) { return cql3_type::kind::DATE; }
        cql3_type::kind operator()(const utf8_type_impl&) { return cql3_type::kind::TEXT; }
        cql3_type::kind operator()(const time_type_impl&) { return cql3_type::kind::TIME; }
        cql3_type::kind operator()(const timestamp_date_base_class&) { return cql3_type::kind::TIMESTAMP; }
        cql3_type::kind operator()(const timeuuid_type_impl&) { return cql3_type::kind::TIMEUUID; }
        cql3_type::kind operator()(const uuid_type_impl&) { return cql3_type::kind::UUID; }
        cql3_type::kind operator()(const varint_type_impl&) { return cql3_type::kind::VARINT; }
        cql3_type::kind operator()(const reversed_type_impl& r) { return get_cql3_kind(*r.underlying_type()); }
        cql3_type::kind operator()(const tuple_type_impl&) { SCYLLA_ASSERT(0 && "no kind for this type"); }
        cql3_type::kind operator()(const collection_type_impl&) { SCYLLA_ASSERT(0 && "no kind for this type"); }
    };
    return visit(t, visitor{});
}

cql3_type::kind_enum_set::prepared cql3_type::get_kind() const {
    return kind_enum_set::prepare(get_cql3_kind(*_type));
}

cql3_type cql3_type::raw::prepare(data_dictionary::database db, const sstring& keyspace) {
    try {
        auto&& ks = db.find_keyspace(keyspace);
        return prepare_internal(keyspace, ks.metadata()->user_types());
    } catch (data_dictionary::no_such_keyspace& nsk) {
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

    virtual sstring to_string() const override {
        return _type.to_string();
    }
public:
    raw_type(cql3_type type)
        : _type{type}
    { }
public:
    virtual cql3_type prepare(data_dictionary::database db, const sstring& keyspace) override {
        return _type;
    }
    cql3_type prepare_internal(const sstring&, const data_dictionary::user_types_metadata&) override {
        return _type;
    }

    virtual bool supports_freezing() const override {
        return false;
    }

    virtual bool is_counter() const override {
        return _type.is_counter();
    }

    virtual bool is_duration() const override {
        return _type.get_type() == duration_type;
    }
};

class cql3_type::raw_collection : public raw {
    const abstract_type::kind _kind;
    shared_ptr<raw> _keys;
    shared_ptr<raw> _values;

    virtual sstring to_string() const override {
        sstring start = is_frozen() ? "frozen<" : "";
        sstring end = is_frozen() ? ">" : "";
        if (_kind == abstract_type::kind::list) {
            return format("{}list<{}>{}", start, _values, end);
        } else if (_kind == abstract_type::kind::set) {
            return format("{}set<{}>{}", start, _values, end);
        } else if (_kind == abstract_type::kind::map) {
            return format("{}map<{}, {}>{}", start, _keys, _values, end);
        }
        abort();
    }
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

    virtual cql3_type prepare_internal(const sstring& keyspace, const data_dictionary::user_types_metadata& user_types) override {
        SCYLLA_ASSERT(_values); // "Got null values type for a collection";

        if (_values->is_counter()) {
            throw exceptions::invalid_request_exception(format("Counters are not allowed inside collections: {}", *this));
        }

        auto result = do_prepare(keyspace, user_types);

        if (!is_frozen() && _values->supports_freezing() && !_values->is_frozen()) {
            throw exceptions::invalid_request_exception(
                    format("Non-frozen user types or collections are not allowed inside collections: {}", *this));
        }
        if (_keys) {
            if (!is_frozen() && _keys->supports_freezing() && !_keys->is_frozen()) {
                throw exceptions::invalid_request_exception(
                        format("Non-frozen user types or collections are not allowed inside collections: {}", *this));
            }
        }

        return result;
    }

    bool references_user_type(const sstring& name) const override {
        return (_keys && _keys->references_user_type(name)) || _values->references_user_type(name);
    }

    bool is_duration() const override {
        return false;
    }

private:
    cql3_type do_prepare(const sstring& keyspace, const data_dictionary::user_types_metadata& user_types) {
        if (_kind == abstract_type::kind::list) {
            return cql3_type(list_type_impl::get_instance(_values->prepare_internal(keyspace, user_types).get_type(), !is_frozen()));
        } else if (_kind == abstract_type::kind::set) {
            if (_values->is_duration()) {
                throw exceptions::invalid_request_exception(format("Durations are not allowed inside sets: {}", *this));
            }
            return cql3_type(set_type_impl::get_instance(_values->prepare_internal(keyspace, user_types).get_type(), !is_frozen()));
        } else if (_kind == abstract_type::kind::map) {
            SCYLLA_ASSERT(_keys); // "Got null keys type for a collection";
            if (_keys->is_duration()) {
                throw exceptions::invalid_request_exception(format("Durations are not allowed as map keys: {}", *this));
            }
            return cql3_type(map_type_impl::get_instance(_keys->prepare_internal(keyspace, user_types).get_type(),
                                                         _values->prepare_internal(keyspace, user_types).get_type(),
                                                         !is_frozen()));
        }
        abort();
    }
};

class cql3_type::raw_ut : public raw {
    ut_name _name;

    virtual sstring to_string() const override {
        if (is_frozen()) {
            return format("frozen<{}>", _name.to_cql_string());
        }

        return _name.to_cql_string();
    }
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

    virtual cql3_type prepare_internal(const sstring& keyspace, const data_dictionary::user_types_metadata& user_types) override {
        if (_name.has_keyspace()) {
            // The provided keyspace is the one of the current statement this is part of. If it's different from the keyspace of
            // the UTName, we reject since we want to limit user types to their own keyspace (see #6643)
            if (!keyspace.empty() && keyspace != _name.get_keyspace()) {
                throw exceptions::invalid_request_exception(fmt::format("Statement on keyspace {} cannot refer to a user type in keyspace {}; "
                                                                   "user types can only be used in the keyspace they are defined in",
                                                                keyspace, _name.get_keyspace()));
            }
        } else {
            _name.set_keyspace(keyspace);
        }
        try {
            data_type type = user_types.get_type(_name.get_user_type_name());
            if (is_frozen()) {
                type = type->freeze();
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

    virtual bool is_user_type() const override {
        return true;
    }
};


class cql3_type::raw_tuple : public raw {
    std::vector<shared_ptr<raw>> _types;

    virtual sstring to_string() const override {
        return format("tuple<{}>", fmt::join(_types, ", "));
    }
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
    virtual cql3_type prepare_internal(const sstring& keyspace, const data_dictionary::user_types_metadata& user_types) override {
        if (!is_frozen()) {
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
};

bool
cql3_type::raw::is_collection() const {
    return false;
}

bool
cql3_type::raw::is_counter() const {
    return false;
}

bool
cql3_type::raw::is_user_type() const {
    return false;
}

bool
cql3_type::raw::is_frozen() const {
    return _frozen;
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
    return ::make_shared<raw_collection>(abstract_type::kind::map, std::move(t1), std::move(t2));
}

shared_ptr<cql3_type::raw>
cql3_type::raw::list(shared_ptr<raw> t) {
    return ::make_shared<raw_collection>(abstract_type::kind::list, nullptr, std::move(t));
}

shared_ptr<cql3_type::raw>
cql3_type::raw::set(shared_ptr<raw> t) {
    return ::make_shared<raw_collection>(abstract_type::kind::set, nullptr, std::move(t));
}

shared_ptr<cql3_type::raw>
cql3_type::raw::tuple(std::vector<shared_ptr<raw>> ts) {
    return ::make_shared<raw_tuple>(std::move(ts));
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

namespace util {

sstring maybe_quote(const sstring& identifier) {
    // quote empty string
    if (identifier.empty()) {
        return "\"\"";
    }

    // string needs no quoting if it matches [a-z][a-z0-9_]*
    // quotes ('"') in the string are doubled
    auto c = identifier[0];
    bool need_quotes = !('a' <= c && c <= 'z');
    size_t num_quotes = 0;
    for (char c : identifier) {
        if (!(('a' <= c && c <= 'z') || ('0' <= c && c <= '9') || (c == '_'))) {
            need_quotes = true;
            num_quotes += (c == '"');
        }
    }

    if (!need_quotes) {
        // A seemingly valid identifier matching [a-z][a-z0-9_]* may still
        // need quoting if it is a CQL keyword, e.g., "to" (see issue #9450).
        // While our parser Cql.g has different production rules for different
        // types of identifiers (column names, table names, etc.), all of
        // these behave identically for alphanumeric strings: they exclude
        // many keywords but allow keywords listed as "unreserved keywords".
        // So we can use any of them, for example cident.
        try {
            cql3::util::do_with_parser(identifier, std::mem_fn(&cql3_parser::CqlParser::cident));
            return identifier;
        } catch(exceptions::syntax_exception&) {
            // This alphanumeric string is not a valid identifier, so fall
            // through to have it quoted:
        }
    }
    if (num_quotes == 0) {
        return make_sstring("\"", identifier, "\"");
    }
    static const boost::regex double_quote_re("\"");
    std::string result;
    result.reserve(2 + identifier.size() + num_quotes);
    result.push_back('"');
    boost::regex_replace(std::back_inserter(result), identifier.begin(), identifier.end(), double_quote_re, "\"\"");
    result.push_back('"');
    return result;
}

template <char C>
static sstring quote_with(const sstring& str) {
    static const std::string quote_str{C};

    // quote empty string
    if (str.empty()) {
        return make_sstring(quote_str, quote_str);
    }
    size_t num_quotes = 0;
    for (char c : str) {
        num_quotes += (c == C);
    }
    if (num_quotes == 0) {
        return make_sstring(quote_str, str, quote_str);
    }

    static const std::string double_quote_str{C, C};
    static const boost::regex quote_re(std::string{C});
    std::string result;
    result.reserve(2 + str.size() + num_quotes);
    result.push_back(C);
    boost::regex_replace(std::back_inserter(result), str.begin(), str.end(), quote_re, double_quote_str);
    result.push_back(C);
    return result;
}

sstring quote(const sstring& identifier) {
    return quote_with<'"'>(identifier);
}

sstring single_quote(const sstring& str) {
    return quote_with<'\''>(str);
}

}

}

