/*
 * Copyright (C) 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

#include "cql3/restrictions/restrictions.hh"
#include "cql3/restrictions/single_column_restriction.hh"
#include "schema_fwd.hh"
#include "types.hh"

namespace cql3 {

namespace restrictions {

/**
 * Sets of single column _restrictions.
 */
class single_column_restrictions : public restrictions {
private:
    /**
     * The comparator used to sort the <code>restriction</code>s.
     */
    struct column_definition_comparator {
        schema_ptr _schema;
        bool operator()(const column_definition* def1, const column_definition* def2) const {
            auto pos1 = _schema->position(*def1);
            auto pos2 = _schema->position(*def2);
            if (pos1 != pos2) {
                return pos1 < pos2;
            }
            // FIXME: shouldn't we use regular column name comparator here? Origin does not...
            return less_unsigned(def1->name(), def2->name());
        }
    };

    /**
     * The _restrictions per column.
     */
public:
    using restrictions_map = std::map<const column_definition*, ::shared_ptr<restriction>, column_definition_comparator>;
private:
    restrictions_map _restrictions;
    bool _is_all_eq = true;
public:
    single_column_restrictions(schema_ptr schema)
        : _restrictions(column_definition_comparator{std::move(schema)})
    { }

#if 0
    @Override
    public final void addIndexExpressionTo(List<IndexExpression> expressions,
                                           QueryOptions options) throws InvalidRequestException
    {
        for (Restriction restriction : _restrictions.values())
            restriction.addIndexExpressionTo(expressions, options);
    }
#endif

    virtual std::vector<const column_definition*> get_column_defs() const override {
        std::vector<const column_definition*> r;
        for (auto&& e : _restrictions) {
            r.push_back(e.first);
        }
        return r;
    }

    virtual bytes_opt value_for(const column_definition& cdef, const query_options& options) const override {
        auto it = _restrictions.find(std::addressof(cdef));
        if (it == _restrictions.end()) {
            return bytes_opt{};
        } else {
            const auto values = std::get<expr::value_list>(possible_lhs_values(&cdef, it->second->expression, options));
            if (values.empty()) {
                return bytes_opt{};
            }
            assert(values.size() == 1);
            return to_bytes(values.front());
        }
    }

    /**
     * Returns the restriction associated to the specified column.
     *
     * @param column_def the column definition
     * @return the restriction associated to the specified column
     */
    ::shared_ptr<restriction> get_restriction(const column_definition& column_def) const {
        auto i = _restrictions.find(&column_def);
        if (i == _restrictions.end()) {
            return {};
        }
        return i->second;
    }

    virtual bool empty() const override {
        return _restrictions.empty();
    }

    virtual uint32_t size() const override {
        return _restrictions.size();
    }

    /**
     * Adds the specified restriction to this set of _restrictions.
     *
     * @param restriction the restriction to add
     * @throws InvalidRequestException if the new restriction cannot be added
     */
    void add_restriction(::shared_ptr<restriction> restriction) {
        if (!find(restriction->expression, expr::oper_t::EQ)) {
            _is_all_eq = false;
        }

        auto i = _restrictions.find(get_the_only_column(restriction->expression).col);
        if (i == _restrictions.end()) {
            _restrictions.emplace_hint(i, get_the_only_column(restriction->expression).col, std::move(restriction));
        } else {
            auto& e = i->second->expression;
            e = make_conjunction(std::move(e), restriction->expression);
        }
    }

    virtual bool has_supporting_index(const secondary_index::secondary_index_manager& index_manager,
                                      expr::allow_local_index allow_local) const override {
        for (auto&& e : _restrictions) {
            if (expr::has_supporting_index(e.second->expression, index_manager, allow_local)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Returns the column after the specified one.
     *
     * @param column_def the column for which the next one need to be found
     * @return the column after the specified one.
     */
    const column_definition* next_column(const column_definition& column_def) const {
        auto i = _restrictions.find(&column_def);
        if (i == _restrictions.end()) {
            return nullptr;
        }
        ++i;
        if (i == _restrictions.end()) {
            return nullptr;
        }
        return i->first;
    }

    /**
     * Returns the definition of the last column.
     *
     * @return the definition of the last column.
     */
    const column_definition* last_column() const {
        if (_restrictions.empty()) {
            return nullptr;
        }
        auto i = _restrictions.end();
        --i;
        return i->first;
    }

    /**
     * Returns the last restriction.
     *
     * @return the last restriction.
     */
    ::shared_ptr<restriction> last_restriction() const {
        if (_restrictions.empty()) {
            return {};
        }
        auto i = _restrictions.end();
        --i;
        return i->second;
    }

    const restrictions_map& restrictions() const {
        return _restrictions;
    }

    /**
     * Checks if the _restrictions contains multiple contains, contains key, or map[key] = value.
     *
     * @return <code>true</code> if the _restrictions contains multiple contains, contains key, or ,
     * map[key] = value; <code>false</code> otherwise
     */
    bool has_multiple_contains() const {
        uint32_t number_of_contains = 0;
        for (auto&& e : _restrictions) {
            number_of_contains += count_if(e.second->expression, expr::is_on_collection);
            if (number_of_contains > 1) {
                return true;
            }
        }
        return number_of_contains > 1;
    }

    bool is_all_eq() const {
        return _is_all_eq;
    }
};

}
}
