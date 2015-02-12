#include "cql3/constants.hh"
#include "cql3/cql3_type.hh"

namespace cql3 {

const ::shared_ptr<term::raw> constants::NULL_LITERAL = ::make_shared<constants::null_literal>();
const ::shared_ptr<terminal> constants::null_literal::NULL_VALUE = ::make_shared<constants::null_literal::null_value>();

std::ostream&
operator<<(std::ostream&out, constants::type t)
{
    switch (t) {
        case constants::type::STRING:  return out << "STRING";
        case constants::type::INTEGER: return out << "INTEGER";
        case constants::type::UUID:    return out << "UUID";
        case constants::type::FLOAT:   return out << "FLOAT";
        case constants::type::BOOLEAN: return out << "BOOLEAN";
        case constants::type::HEX:     return out << "HEX";
    };
    assert(0);
}

bytes
constants::literal::parsed_value(::shared_ptr<abstract_type> validator)
{
    try {
        if (_type == type::HEX && validator == bytes_type) {
            auto v = static_cast<sstring_view>(_text);
            v.remove_prefix(2);
            return validator->from_string(v);
        }
        if (validator->is_counter()) {
            return long_type->from_string(_text);
        }
        return validator->from_string(_text);
    } catch (const exceptions::marshal_exception& e) {
        throw exceptions::invalid_request_exception(e.what());
    }
}

assignment_testable::test_result
constants::literal::test_assignment(const sstring& keyspace, ::shared_ptr<column_specification> receiver)
{
    auto receiver_type = receiver->type->as_cql3_type();
    if (receiver_type->is_collection()) {
        return test_result::NOT_ASSIGNABLE;
    }
    if (!receiver_type->is_native()) {
        return test_result::WEAKLY_ASSIGNABLE;
    }
    auto kind = static_cast<native_cql3_type*>(receiver_type.get())->get_kind();
    switch (_type) {
        case type::STRING:
            if (native_cql3_type::kind_enum_set::frozen<
                    native_cql3_type::kind::ASCII,
                    native_cql3_type::kind::TEXT,
                    native_cql3_type::kind::INET,
                    native_cql3_type::kind::VARCHAR,
                    native_cql3_type::kind::TIMESTAMP>::contains(kind)) {
                return assignment_testable::test_result::WEAKLY_ASSIGNABLE;
            }
            break;
        case type::INTEGER:
            if (native_cql3_type::kind_enum_set::frozen<
                    native_cql3_type::kind::BIGINT,
                    native_cql3_type::kind::COUNTER,
                    native_cql3_type::kind::DECIMAL,
                    native_cql3_type::kind::DOUBLE,
                    native_cql3_type::kind::FLOAT,
                    native_cql3_type::kind::INT,
                    native_cql3_type::kind::TIMESTAMP,
                    native_cql3_type::kind::VARINT>::contains(kind)) {
                return assignment_testable::test_result::WEAKLY_ASSIGNABLE;
            }
            break;
        case type::UUID:
            if (native_cql3_type::kind_enum_set::frozen<
                    native_cql3_type::kind::UUID,
                    native_cql3_type::kind::TIMEUUID>::contains(kind)) {
                return assignment_testable::test_result::WEAKLY_ASSIGNABLE;
            }
            break;
        case type::FLOAT:
            if (native_cql3_type::kind_enum_set::frozen<
                    native_cql3_type::kind::DECIMAL,
                    native_cql3_type::kind::DOUBLE,
                    native_cql3_type::kind::FLOAT>::contains(kind)) {
                return assignment_testable::test_result::WEAKLY_ASSIGNABLE;
            }
            break;
        case type::BOOLEAN:
            if (kind == native_cql3_type::kind_enum_set::prepare<native_cql3_type::kind::BOOLEAN>()) {
                return assignment_testable::test_result::WEAKLY_ASSIGNABLE;
            }
            break;
        case type::HEX:
            if (kind == native_cql3_type::kind_enum_set::prepare<native_cql3_type::kind::BLOB>()) {
                return assignment_testable::test_result::WEAKLY_ASSIGNABLE;
            }
            break;
    }
    return assignment_testable::test_result::NOT_ASSIGNABLE;
}

::shared_ptr<term>
constants::literal::prepare(const sstring& keyspace, ::shared_ptr<column_specification> receiver)
{
    if (!is_assignable(test_assignment(keyspace, receiver))) {
        throw exceptions::invalid_request_exception(sprint("Invalid %s constant (%s) for \"%s\" of type %s",
            _type, _text, *receiver->name, receiver->type->as_cql3_type()->to_string()));
    }
    return ::make_shared<value>(std::experimental::make_optional(parsed_value(receiver->type)));
}

}
