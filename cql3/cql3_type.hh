/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Modified by Cloudius Systems
 *
 * Copyright 2015 Cloudius Systems
 */

#pragma once

#include "types.hh"
#include "exceptions/exceptions.hh"
#include <iosfwd>
#include "enum_set.hh"

namespace cql3 {

class cql3_type final {
    sstring _name;
    data_type _type;
    bool _native;
public:
    cql3_type(sstring name, data_type type, bool native = true)
            : _name(std::move(name)), _type(std::move(type)), _native(native) {}
    bool is_collection() const { return _type->is_collection(); }
    bool is_native() const { return _native; }
    data_type get_type() const { return _type; }
    sstring to_string() const { return _name; }

    // For UserTypes, we need to know the current keyspace to resolve the
    // actual type used, so Raw is a "not yet prepared" CQL3Type.
    class raw {
    public:
        bool _frozen = false;
        virtual bool supports_freezing() const = 0;

        virtual bool is_collection() const {
            return false;
        }

        virtual bool is_counter() const {
            return false;
        }

        virtual std::experimental::optional<sstring> keyspace() const {
            return std::experimental::optional<sstring>{};
        }

        virtual void freeze() {
            sstring message = sprint("frozen<> is only allowed on collections, tuples, and user-defined types (got %s)", to_string());
            throw exceptions::invalid_request_exception(message);
        }

        virtual shared_ptr<cql3_type> prepare(const sstring& keyspace) = 0;

        static shared_ptr<raw> from(shared_ptr<cql3_type> type) {
            return ::make_shared<raw_type>(type);
        }

#if 0
        public static Raw userType(UTName name)
        {
            return new RawUT(name);
        }
#endif

        static shared_ptr<raw> map(shared_ptr<raw> t1, shared_ptr<raw> t2) {
            return make_shared(raw_collection(&collection_type_impl::kind::map, std::move(t1), std::move(t2)));
        }

        static shared_ptr<raw> list(shared_ptr<raw> t) {
            return make_shared(raw_collection(&collection_type_impl::kind::list, {}, std::move(t)));
        }

        static shared_ptr<raw> set(shared_ptr<raw> t) {
            return make_shared(raw_collection(&collection_type_impl::kind::set, {}, std::move(t)));
        }

        static shared_ptr<raw> tuple(std::vector<shared_ptr<raw>> ts) {
            return make_shared(raw_tuple(std::move(ts)));
        }

#if 0
        public static Raw frozen(CQL3Type.Raw t) throws InvalidRequestException
        {
            t.freeze();
            return t;
        }
#endif

        virtual sstring to_string() const = 0;

        friend std::ostream& operator<<(std::ostream& os, const raw& r);
    };

private:
    class raw_type : public raw {
    private:
        shared_ptr<cql3_type> _type;
    public:
        raw_type(shared_ptr<cql3_type> type)
            : _type{type}
        { }
    public:
        virtual shared_ptr<cql3_type> prepare(const sstring& keyspace) {
            return _type;
        }

        virtual bool supports_freezing() const {
            return false;
        }

        virtual bool is_counter() const {
            throw std::runtime_error("not implemented");
#if 0
            return type == Native.COUNTER;
#endif
        }

        virtual sstring to_string() const {
            return _type->to_string();
        }
    };

    class raw_collection : public raw {
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

        virtual shared_ptr<cql3_type> prepare(const sstring& keyspace) override {
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
                return make_shared(cql3_type(to_string(), list_type_impl::get_instance(_values->prepare(keyspace)->get_type(), !_frozen), false));
            } else if (_kind == &collection_type_impl::kind::set) {
                return make_shared(cql3_type(to_string(), set_type_impl::get_instance(_values->prepare(keyspace)->get_type(), !_frozen), false));
            } else if (_kind == &collection_type_impl::kind::map) {
                assert(_keys); // "Got null keys type for a collection";
                return make_shared(cql3_type(to_string(), map_type_impl::get_instance(_keys->prepare(keyspace)->get_type(), _values->prepare(keyspace)->get_type(), !_frozen), false));
            }
            abort();
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

#if 0
        private static class RawUT extends Raw
        {
            private final UTName name;

            private RawUT(UTName name)
            {
                this.name = name;
            }

            public String keyspace()
            {
                return name.getKeyspace();
            }

            public void freeze()
            {
                frozen = true;
            }

            public CQL3Type prepare(String keyspace) throws InvalidRequestException
            {
                if (name.hasKeyspace())
                {
                    // The provided keyspace is the one of the current statement this is part of. If it's different from the keyspace of
                    // the UTName, we reject since we want to limit user types to their own keyspace (see #6643)
                    if (keyspace != null && !keyspace.equals(name.getKeyspace()))
                        throw new InvalidRequestException(String.format("Statement on keyspace %s cannot refer to a user type in keyspace %s; "
                                                                        + "user types can only be used in the keyspace they are defined in",
                                                                        keyspace, name.getKeyspace()));
                }
                else
                {
                    name.setKeyspace(keyspace);
                }

                KSMetaData ksm = Schema.instance.getKSMetaData(name.getKeyspace());
                if (ksm == null)
                    throw new InvalidRequestException("Unknown keyspace " + name.getKeyspace());
                UserType type = ksm.userTypes.getType(name.getUserTypeName());
                if (type == null)
                    throw new InvalidRequestException("Unknown type " + name);

                if (!frozen)
                    throw new InvalidRequestException("Non-frozen User-Defined types are not supported, please use frozen<>");

                return new UserDefined(name.toString(), type);
            }

            protected boolean supportsFreezing()
            {
                return true;
            }

            @Override
            public String toString()
            {
                return name.toString();
            }
        }
#endif

        class raw_tuple : public raw {
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
            virtual shared_ptr<cql3_type> prepare(const sstring& keyspace) override {
                if (!_frozen) {
                    freeze();
                }
                std::vector<data_type> ts;
                for (auto&& t : _types) {
                    if (t->is_counter()) {
                        throw exceptions::invalid_request_exception("Counters are not allowed inside tuples");
                    }
                    ts.push_back(t->prepare(keyspace)->get_type());
                }
                return make_cql3_tuple_type(tuple_type_impl::get_instance(std::move(ts)));
            }
            virtual sstring to_string() const override {
                return sprint("tuple<%s>", join(", ", _types));
            }
        };

    friend std::ostream& operator<<(std::ostream& os, const cql3_type& t) {
        return os << t.to_string();
    }

public:
    enum class kind : int8_t {
        ASCII, BIGINT, BLOB, BOOLEAN, COUNTER, DECIMAL, DOUBLE, FLOAT, INT, INET, TEXT, TIMESTAMP, UUID, VARCHAR, VARINT, TIMEUUID
    };
    using kind_enum = super_enum<kind,
        kind::ASCII,
        kind::BIGINT,
        kind::BLOB,
        kind::BOOLEAN,
        kind::COUNTER,
        kind::DECIMAL,
        kind::DOUBLE,
        kind::FLOAT,
        kind::INET,
        kind::INT,
        kind::TEXT,
        kind::TIMESTAMP,
        kind::UUID,
        kind::VARCHAR,
        kind::VARINT,
        kind::TIMEUUID>;
    using kind_enum_set = enum_set<kind_enum>;
private:
    std::experimental::optional<kind_enum_set::prepared> _kind;
    static shared_ptr<cql3_type> make(sstring name, data_type type, kind kind_) {
        return make_shared<cql3_type>(std::move(name), std::move(type), kind_);
    }
public:
    static thread_local shared_ptr<cql3_type> ascii;
    static thread_local shared_ptr<cql3_type> bigint;
    static thread_local shared_ptr<cql3_type> blob;
    static thread_local shared_ptr<cql3_type> boolean;
    static thread_local shared_ptr<cql3_type> double_;
    static thread_local shared_ptr<cql3_type> float_;
    static thread_local shared_ptr<cql3_type> int_;
    static thread_local shared_ptr<cql3_type> text;
    static thread_local shared_ptr<cql3_type> timestamp;
    static thread_local shared_ptr<cql3_type> uuid;
    static thread_local shared_ptr<cql3_type> varchar;
    static thread_local shared_ptr<cql3_type> timeuuid;
    static thread_local shared_ptr<cql3_type> inet;

#if 0
        COUNTER  (CounterColumnType.instance),
        DECIMAL  (DecimalType.instance),
        VARINT   (IntegerType.instance),
#endif
    static const std::vector<shared_ptr<cql3_type>>& values();
public:
    cql3_type(sstring name, data_type type, kind kind_)
        : _name(std::move(name)), _type(std::move(type)), _native(true), _kind(kind_enum_set::prepare(kind_)) {
    }
    kind_enum_set::prepared get_kind() const {
        assert(_kind);
        return *_kind;
    }
};

shared_ptr<cql3_type> make_cql3_tuple_type(shared_ptr<tuple_type_impl> t);

#if 0
    public static class Custom implements CQL3Type
    {
        private final AbstractType<?> type;

        public Custom(AbstractType<?> type)
        {
            this.type = type;
        }

        public Custom(String className) throws SyntaxException, ConfigurationException
        {
            this(TypeParser.parse(className));
        }

        public boolean isCollection()
        {
            return false;
        }

        public AbstractType<?> getType()
        {
            return type;
        }

        @Override
        public final boolean equals(Object o)
        {
            if(!(o instanceof Custom))
                return false;

            Custom that = (Custom)o;
            return type.equals(that.type);
        }

        @Override
        public final int hashCode()
        {
            return type.hashCode();
        }

        @Override
        public String toString()
        {
            return "'" + type + "'";
        }
    }

    public static class Collection implements CQL3Type
    {
        private final CollectionType type;

        public Collection(CollectionType type)
        {
            this.type = type;
        }

        public AbstractType<?> getType()
        {
            return type;
        }

        public boolean isCollection()
        {
            return true;
        }

        @Override
        public final boolean equals(Object o)
        {
            if(!(o instanceof Collection))
                return false;

            Collection that = (Collection)o;
            return type.equals(that.type);
        }

        @Override
        public final int hashCode()
        {
            return type.hashCode();
        }

        @Override
        public String toString()
        {
            boolean isFrozen = !this.type.isMultiCell();
            StringBuilder sb = new StringBuilder(isFrozen ? "frozen<" : "");
            switch (type.kind)
            {
                case LIST:
                    AbstractType<?> listType = ((ListType)type).getElementsType();
                    sb.append("list<").append(listType.asCQL3Type());
                    break;
                case SET:
                    AbstractType<?> setType = ((SetType)type).getElementsType();
                    sb.append("set<").append(setType.asCQL3Type());
                    break;
                case MAP:
                    AbstractType<?> keysType = ((MapType)type).getKeysType();
                    AbstractType<?> valuesType = ((MapType)type).getValuesType();
                    sb.append("map<").append(keysType.asCQL3Type()).append(", ").append(valuesType.asCQL3Type());
                    break;
                default:
                    throw new AssertionError();
            }
            sb.append(">");
            if (isFrozen)
                sb.append(">");
            return sb.toString();
        }
    }

    public static class UserDefined implements CQL3Type
    {
        // Keeping this separatly from type just to simplify toString()
        private final String name;
        private final UserType type;

        private UserDefined(String name, UserType type)
        {
            this.name = name;
            this.type = type;
        }

        public static UserDefined create(UserType type)
        {
            return new UserDefined(UTF8Type.instance.compose(type.name), type);
        }

        public boolean isCollection()
        {
            return false;
        }

        public AbstractType<?> getType()
        {
            return type;
        }

        @Override
        public final boolean equals(Object o)
        {
            if(!(o instanceof UserDefined))
                return false;

            UserDefined that = (UserDefined)o;
            return type.equals(that.type);
        }

        @Override
        public final int hashCode()
        {
            return type.hashCode();
        }

        @Override
        public String toString()
        {
            return name;
        }
    }

    public static class Tuple implements CQL3Type
    {
        private final TupleType type;

        private Tuple(TupleType type)
        {
            this.type = type;
        }

        public static Tuple create(TupleType type)
        {
            return new Tuple(type);
        }

        public boolean isCollection()
        {
            return false;
        }

        public AbstractType<?> getType()
        {
            return type;
        }

        @Override
        public final boolean equals(Object o)
        {
            if(!(o instanceof Tuple))
                return false;

            Tuple that = (Tuple)o;
            return type.equals(that.type);
        }

        @Override
        public final int hashCode()
        {
            return type.hashCode();
        }

        @Override
        public String toString()
        {
            StringBuilder sb = new StringBuilder();
            sb.append("tuple<");
            for (int i = 0; i < type.size(); i++)
            {
                if (i > 0)
                    sb.append(", ");
                sb.append(type.type(i).asCQL3Type());
            }
            sb.append(">");
            return sb.toString();
        }
    }
#endif

}
