/*
 * Modified by ScyllaDB
 *
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (LicenseRef-ScyllaDB-Source-Available-1.0 and Apache-2.0)
 */

#pragma once

#include "types/types.hh"
#include "data_dictionary/data_dictionary.hh"
#include <iosfwd>
#include "enum_set.hh"

namespace data_dictionary {
class database;
class user_types_metadata;
}
namespace auth {
class resource;
}

namespace cql3 {

class ut_name;

class cql3_type final {
    data_type _type;
public:
    cql3_type(data_type type) : _type(std::move(type)) {}
    bool is_collection() const { return _type->is_collection(); }
    bool is_counter() const { return _type->is_counter(); }
    bool is_native() const { return _type->is_native(); }
    bool is_user_type() const { return _type->is_user_type(); }
    bool is_vector() const { return _type->is_vector(); }
    data_type get_type() const { return _type; }
    const sstring& to_string() const { return _type->cql3_type_name(); }

    // For UserTypes, we need to know the current keyspace to resolve the
    // actual type used, so Raw is a "not yet prepared" CQL3Type.
    class raw {
        virtual sstring to_string() const = 0;
    protected:
        bool _frozen = false;
    public:
        virtual ~raw() {}
        virtual bool supports_freezing() const = 0;
        virtual bool is_collection() const;
        virtual bool is_counter() const;
        virtual bool is_duration() const;
        virtual bool is_user_type() const;
        bool is_frozen() const;
        virtual bool references_user_type(const sstring&) const;
        virtual std::optional<sstring> keyspace() const;
        virtual void freeze();
        virtual cql3_type prepare_internal(const sstring& keyspace, const data_dictionary::user_types_metadata&) = 0;
        virtual cql3_type prepare(data_dictionary::database db, const sstring& keyspace);
        static shared_ptr<raw> from(cql3_type type);
        static shared_ptr<raw> user_type(ut_name name);
        static shared_ptr<raw> map(shared_ptr<raw> t1, shared_ptr<raw> t2);
        static shared_ptr<raw> list(shared_ptr<raw> t);
        static shared_ptr<raw> set(shared_ptr<raw> t);
        static shared_ptr<raw> tuple(std::vector<shared_ptr<raw>> ts);
        static shared_ptr<raw> vector(shared_ptr<raw> t, size_t dimension);
        static shared_ptr<raw> frozen(shared_ptr<raw> t);
        friend sstring format_as(const raw& r) {
            return r.to_string();
        }
        friend class auth::resource;
    };

private:
    class raw_type;
    class raw_collection;
    class raw_ut;
    class raw_tuple;
    class raw_vector;
    friend std::string_view format_as(const cql3_type& t) {
        return t.to_string();
    }

public:
    enum class kind : int8_t {
        ASCII, BIGINT, BLOB, BOOLEAN, COUNTER, DECIMAL, DOUBLE, EMPTY, FLOAT, INT, SMALLINT, TINYINT, INET, TEXT, TIMESTAMP, UUID, VARINT, TIMEUUID, DATE, TIME, DURATION
    };
    using kind_enum = super_enum<kind,
        kind::ASCII,
        kind::BIGINT,
        kind::BLOB,
        kind::BOOLEAN,
        kind::COUNTER,
        kind::DECIMAL,
        kind::DOUBLE,
        kind::EMPTY,
        kind::FLOAT,
        kind::INET,
        kind::INT,
        kind::SMALLINT,
        kind::TINYINT,
        kind::TEXT,
        kind::TIMESTAMP,
        kind::UUID,
        kind::VARINT,
        kind::TIMEUUID,
        kind::DATE,
        kind::TIME,
        kind::DURATION>;
    using kind_enum_set = enum_set<kind_enum>;

    static thread_local cql3_type ascii;
    static thread_local cql3_type bigint;
    static thread_local cql3_type blob;
    static thread_local cql3_type boolean;
    static thread_local cql3_type double_;
    static thread_local cql3_type empty;
    static thread_local cql3_type float_;
    static thread_local cql3_type int_;
    static thread_local cql3_type smallint;
    static thread_local cql3_type text;
    static thread_local cql3_type timestamp;
    static thread_local cql3_type tinyint;
    static thread_local cql3_type uuid;
    static thread_local cql3_type timeuuid;
    static thread_local cql3_type date;
    static thread_local cql3_type time;
    static thread_local cql3_type inet;
    static thread_local cql3_type varint;
    static thread_local cql3_type decimal;
    static thread_local cql3_type counter;
    static thread_local cql3_type duration;

    static const std::vector<cql3_type>& values();
public:
    kind_enum_set::prepared get_kind() const;
};

inline bool operator==(const cql3_type& a, const cql3_type& b) {
    return a.get_type() == b.get_type();
}

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
        // Keeping this separately from type just to simplify toString()
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

template <>
struct fmt::formatter<cql3::cql3_type>: fmt::formatter<string_view> {
    auto format(const cql3::cql3_type& t, fmt::format_context& ctx) const {
        return formatter<string_view>::format(format_as(t), ctx);
    }
};
