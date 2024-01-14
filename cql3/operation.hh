/*
 * Copyright (C) 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

#include <seastar/core/shared_ptr.hh>
#include "cql3/cql3_type.hh"
#include "data_dictionary/data_dictionary.hh"
#include "update_parameters.hh"
#include "cql3/column_identifier.hh"
#include "cql3/expr/expression.hh"
#include "cql3/expr/unset.hh"

#include <optional>

namespace cql3 {

class update_parameters;

/**
 * An UPDATE or DELETE operation.
 *
 * For UPDATE this includes:
 *   - setting a constant
 *   - counter operations
 *   - collections operations
 * and for DELETE:
 *   - deleting a column
 *   - deleting an element of collection column
 *
 * Fine grained operation are obtained from their raw counterpart (Operation.Raw, which
 * correspond to a parsed, non-checked operation) by provided the receiver for the operation.
 */
class operation {
public:
    // the column the operation applies to
    // We can hold a reference because all operations have life bound to their statements and
    // statements pin the schema.
    const column_definition& column;

protected:
    // Value involved in the operation. In theory this should not be here since some operation
    // may require none of more than one expression, but most need 1 so it simplify things a bit.
    std::optional<expr::expression> _e;

    // A guard to check if the operation should be skipped due to unset operand.
    expr::unset_bind_variable_guard _unset_guard;
public:
    operation(const column_definition& column_, std::optional<expr::expression> e, expr::unset_bind_variable_guard ubvg)
        : column{column_}
        , _e(std::move(e))
        , _unset_guard(std::move(ubvg))
    { }

    virtual ~operation() {}

    virtual bool is_raw_counter_shard_write() const {
        return false;
    }

    /**
    * @return whether the operation requires a read of the previous value to be executed
    * (only lists setterByIdx, discard and discardByIdx requires that).
    */
    virtual bool requires_read() const {
        return false;
    }

    /**
     * Collects the column specification for the bind variables of this operation.
     *
     * @param meta the list of column specification where to collect the
     * bind variables of this term in.
     */
    virtual void fill_prepare_context(prepare_context& ctx);

    /**
     * Execute the operation. Check should_skip_operation() first.
     */
    virtual void execute(mutation& m, const clustering_key_prefix& prefix, const update_parameters& params) = 0;

    bool should_skip_operation(const query_options& qo) const {
        return _unset_guard.is_unset(qo);
    }

    virtual expr::expression prepare_new_value_for_broadcast_tables() const;

    /**
     * A parsed raw UPDATE operation.
     *
     * This can be one of:
     *   - Setting a value: c = v
     *   - Setting an element of a collection: c[x] = v
     *   - Setting a field of a user-defined type: c.x = v
     *   - An addition/subtraction to a variable: c = c +/- v (where v can be a collection literal)
     *   - An prepend operation: c = v + c
     */
    class raw_update {
    public:
        virtual ~raw_update() {}

        /**
         * This method validates the operation (i.e. validate it is well typed)
         * based on the specification of the receiver of the operation.
         *
         * It returns an Operation which can be though as post-preparation well-typed
         * Operation.
         *
         * @param receiver the "column" this operation applies to. Note that
         * contrarly to the method of same name in raw expression, the receiver should always
         * be a true column.
         * @return the prepared update operation.
         */
        virtual ::shared_ptr<operation> prepare(data_dictionary::database db, const sstring& keyspace, const column_definition& receiver) const = 0;

        /**
         * @return whether this operation can be applied alongside the {@code
         * other} update (in the same UPDATE statement for the same column).
         */
        virtual bool is_compatible_with(const std::unique_ptr<raw_update>& other) const = 0;
    };

    /**
     * A parsed raw DELETE operation.
     *
     * This can be one of:
     *   - Deleting a column
     *   - Deleting an element of a collection
     *   - Deleting a field of a user-defined type
     */
    class raw_deletion {
    public:
        virtual ~raw_deletion() = default;

        /**
         * The name of the column affected by this delete operation.
         */
        virtual const column_identifier::raw& affected_column() const = 0;

        /**
         * This method validates the operation (i.e. validate it is well typed)
         * based on the specification of the column affected by the operation (i.e the
         * one returned by affectedColumn()).
         *
         * It returns an Operation which can be though as post-preparation well-typed
         * Operation.
         *
         * @param receiver the "column" this operation applies to.
         * @return the prepared delete operation.
         */
        virtual ::shared_ptr<operation> prepare(data_dictionary::database db, const sstring& keyspace, const column_definition& receiver) const = 0;
    };

    class set_value;
    class set_counter_value_from_tuple_list;

    class set_element : public raw_update {
        const expr::expression _selector;
        const expr::expression _value;
        const bool _by_uuid;
    private:
        sstring to_string(const column_definition& receiver) const;
    public:
        set_element(expr::expression selector, expr::expression value, bool by_uuid = false)
            : _selector(std::move(selector)), _value(std::move(value)), _by_uuid(by_uuid) {
        }

        virtual shared_ptr<operation> prepare(data_dictionary::database db, const sstring& keyspace, const column_definition& receiver) const override;

        virtual bool is_compatible_with(const std::unique_ptr<raw_update>& other) const override;
    };

    // Set a single field inside a user-defined type.
    class set_field : public raw_update {
        const shared_ptr<column_identifier> _field;
        const expr::expression _value;
    private:
        sstring to_string(const column_definition& receiver) const;
    public:
        set_field(shared_ptr<column_identifier> field, expr::expression value)
            : _field(std::move(field)), _value(std::move(value)) {
        }

        virtual shared_ptr<operation> prepare(data_dictionary::database db, const sstring& keyspace, const column_definition& receiver) const override;

        virtual bool is_compatible_with(const std::unique_ptr<raw_update>& other) const override;
    };

    // Delete a single field inside a user-defined type.
    // Equivalent to setting the field to null.
    class field_deletion : public raw_deletion {
        const shared_ptr<column_identifier::raw> _id;
        const shared_ptr<column_identifier> _field;
    public:
        field_deletion(shared_ptr<column_identifier::raw> id, shared_ptr<column_identifier> field)
                : _id(std::move(id)), _field(std::move(field)) {
        }

        virtual const column_identifier::raw& affected_column() const override;

        virtual shared_ptr<operation> prepare(data_dictionary::database db, const sstring& keyspace, const column_definition& receiver) const override;
    };

    class addition : public raw_update {
        const expr::expression _value;
    private:
        sstring to_string(const column_definition& receiver) const;
    public:
        addition(expr::expression value)
                : _value(std::move(value)) {
        }

        virtual shared_ptr<operation> prepare(data_dictionary::database db, const sstring& keyspace, const column_definition& receiver) const override;

        virtual bool is_compatible_with(const std::unique_ptr<raw_update>& other) const override;
    };

    class subtraction : public raw_update {
        const expr::expression _value;
    private:
        sstring to_string(const column_definition& receiver) const;
    public:
        subtraction(expr::expression value)
                : _value(std::move(value)) {
        }

        virtual shared_ptr<operation> prepare(data_dictionary::database db, const sstring& keyspace, const column_definition& receiver) const override;

        virtual bool is_compatible_with(const std::unique_ptr<raw_update>& other) const override;
    };

    class prepend : public raw_update {
        expr::expression _value;
    private:
        sstring to_string(const column_definition& receiver) const;
    public:
        prepend(expr::expression value)
                : _value(std::move(value)) {
        }

        virtual shared_ptr<operation> prepare(data_dictionary::database db, const sstring& keyspace, const column_definition& receiver) const override;

        virtual bool is_compatible_with(const std::unique_ptr<raw_update>& other) const override;
    };

    class column_deletion;

    class element_deletion : public raw_deletion {
        shared_ptr<column_identifier::raw> _id;
        expr::expression _element;
    public:
        element_deletion(shared_ptr<column_identifier::raw> id, expr::expression element)
                : _id(std::move(id)), _element(std::move(element)) {
        }
        virtual const column_identifier::raw& affected_column() const override;
        virtual shared_ptr<operation> prepare(data_dictionary::database db, const sstring& keyspace, const column_definition& receiver) const override;
    };
};

class operation_skip_if_unset : public operation {
public:
    operation_skip_if_unset(const column_definition& column, expr::expression e)
            : operation(column, e, expr::unset_bind_variable_guard(e)) {
    }
};

class operation_no_unset_support : public operation {
public:
    operation_no_unset_support(const column_definition& column, std::optional<expr::expression> e)
            : operation(column, std::move(e), expr::unset_bind_variable_guard(std::nullopt)) {
    }
};

}
