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
 * Copyright 2015 Cloudius Systems
 *
 * Modified by Cloudius Systems
 */

#include "cql3/selection/selection.hh"
#include "cql3/selection/selector_factories.hh"

namespace cql3 {

namespace selection {

// Special cased selection for when no function is used (this save some allocations).
class simple_selection : public selection {
private:
    const bool _is_wildcard;
public:
    static ::shared_ptr<simple_selection> make(schema_ptr schema, std::vector<const column_definition*> columns, bool is_wildcard) {
        std::vector<::shared_ptr<column_specification>> metadata;
        metadata.reserve(columns.size());
        for (auto&& col : columns) {
            metadata.emplace_back(col->column_specification);
        }
        return ::make_shared<simple_selection>(schema, std::move(columns), std::move(metadata), is_wildcard);
    }

    /*
     * In theory, even a simple selection could have multiple time the same column, so we
     * could filter those duplicate out of columns. But since we're very unlikely to
     * get much duplicate in practice, it's more efficient not to bother.
     */
    simple_selection(schema_ptr schema, std::vector<const column_definition*> columns,
        std::vector<::shared_ptr<column_specification>> metadata, bool is_wildcard)
            : selection(schema, std::move(columns), std::move(metadata), false, false)
            , _is_wildcard(is_wildcard)
    { }

    virtual bool is_wildcard() const override { return _is_wildcard; }
    virtual bool is_aggregate() const override { return false; }
protected:
    class simple_selectors : public selectors {
    private:
        std::vector<bytes_opt> _current;
    public:
        virtual void reset() override {
            _current.clear();
        }

        virtual std::vector<bytes_opt> get_output_row(int32_t protocol_version) override {
            return std::move(_current);
        }

        virtual void add_input_row(int32_t protocol_version, result_set_builder& rs) override {
            _current = std::move(rs.current);
        }

        virtual bool is_aggregate() {
            return false;
        }
    };

    std::unique_ptr<selectors> new_selectors() {
        return std::make_unique<simple_selectors>();
    }
};

class selection_with_processing : public selection {
private:
    ::shared_ptr<selector_factories> _factories;
public:
    selection_with_processing(schema_ptr schema, std::vector<const column_definition*> columns,
            std::vector<::shared_ptr<column_specification>> metadata, ::shared_ptr<selector_factories> factories)
        : selection(schema, std::move(columns), std::move(metadata),
            factories->contains_write_time_selector_factory(),
            factories->contains_ttl_selector_factory())
        , _factories(std::move(factories))
    {
        if (_factories->does_aggregation() && !_factories->contains_only_aggregate_functions()) {
            throw exceptions::invalid_request_exception("the select clause must either contains only aggregates or none");
        }
    }

    virtual bool uses_function(const sstring& ks_name, const sstring& function_name) const override {
        return _factories->uses_function(ks_name, function_name);
    }

    virtual uint32_t add_column_for_ordering(const column_definition& c) override {
        uint32_t index = selection::add_column_for_ordering(c);
        _factories->add_selector_for_ordering(c, index);
        return index;
    }

    virtual bool is_aggregate() const override {
        return _factories->contains_only_aggregate_functions();
    }
protected:
    class selectors_with_processing : public selectors {
    private:
        ::shared_ptr<selector_factories> _factories;
        std::vector<::shared_ptr<selector>> _selectors;
    public:
        selectors_with_processing(::shared_ptr<selector_factories> factories)
            : _factories(std::move(factories))
            , _selectors(_factories->new_instances())
        { }

        virtual void reset() override {
            for (auto&& s : _selectors) {
                s->reset();
            }
        }

        virtual bool is_aggregate() override {
            return _factories->contains_only_aggregate_functions();
        }

        virtual std::vector<bytes_opt> get_output_row(int32_t protocol_version) override {
            std::vector<bytes_opt> output_row;
            output_row.reserve(_selectors.size());
            for (auto&& s : _selectors) {
                output_row.emplace_back(s->get_output(protocol_version));
            }
            return output_row;
        }

        virtual void add_input_row(int32_t protocol_version, result_set_builder& rs) {
            for (auto&& s : _selectors) {
                s->add_input(protocol_version, rs);
            }
        }
    };

    std::unique_ptr<selectors> new_selectors() {
        return std::make_unique<selectors_with_processing>(_factories);
    }
};

::shared_ptr<selection> selection::wildcard(schema_ptr schema) {
    return simple_selection::make(schema, column_definition::vectorize(schema->all_columns_in_select_order()), true);
}

::shared_ptr<selection> selection::for_columns(schema_ptr schema, std::vector<const column_definition*> columns) {
    return simple_selection::make(schema, std::move(columns), false);
}

uint32_t selection::add_column_for_ordering(const column_definition& c) {
    _columns.push_back(&c);
    _metadata->add_non_serialized_column(c.column_specification);
    return _columns.size() - 1;
}

::shared_ptr<selection> selection::from_selectors(schema_ptr schema, const std::vector<::shared_ptr<raw_selector>>& raw_selectors) {
    std::vector<const column_definition*> defs;

    ::shared_ptr<selector_factories> factories =
        selector_factories::create_factories_and_collect_column_definitions(
            raw_selector::to_selectables(raw_selectors, schema), schema, defs);

    auto metadata = collect_metadata(schema, raw_selectors, *factories);
    if (processes_selection(raw_selectors)) {
        return ::make_shared<selection_with_processing>(schema, std::move(defs), std::move(metadata), std::move(factories));
    } else {
        return ::make_shared<simple_selection>(schema, std::move(defs), std::move(metadata), false);
    }
}

std::vector<::shared_ptr<column_specification>>
selection::collect_metadata(schema_ptr schema, const std::vector<::shared_ptr<raw_selector>>& raw_selectors,
        const selector_factories& factories) {
    std::vector<::shared_ptr<column_specification>> r;
    r.reserve(raw_selectors.size());
    auto i = raw_selectors.begin();
    for (auto&& factory : factories) {
        ::shared_ptr<column_specification> col_spec = factory->get_column_specification(schema);
        ::shared_ptr<column_identifier> alias = (*i++)->alias;
        r.push_back(alias ? col_spec->with_alias(alias) : col_spec);
    }
    return r;
}

result_set_builder::result_set_builder(selection& s, db_clock::time_point now, int32_t protocol_version)
    : _result_set(std::make_unique<result_set>(::make_shared<metadata>(*(s.get_result_metadata()))))
    , _selectors(s.new_selectors())
    , _now(now)
    , _protocol_version(protocol_version)
{
    if (s._collect_timestamps) {
        _timestamps.resize(s._columns.size(), 0);
    }
    if (s._collect_TTLs) {
        _ttls.resize(s._columns.size(), 0);
    }
}

}

}