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

#pragma once

#include "bytes.hh"
#include "schema.hh"
#include "cql3/column_specification.hh"
#include "exceptions/exceptions.hh"
#include "cql3/result_set.hh"
#include "cql3/selection/raw_selector.hh"
#include "cql3/selection/selector_factories.hh"
#include "unimplemented.hh"

namespace cql3 {

namespace selection {

class selectors {
public:
    virtual ~selectors() {}

    virtual bool is_aggregate() = 0;

    /**
    * Adds the current row of the specified <code>ResultSetBuilder</code>.
    *
    * @param rs the <code>ResultSetBuilder</code>
    * @throws InvalidRequestException
    */
    virtual void add_input_row(serialization_format sf, result_set_builder& rs) = 0;

    virtual std::vector<bytes_opt> get_output_row(serialization_format sf) = 0;

    virtual void reset() = 0;
};

class selection {
private:
    schema_ptr _schema;
    std::vector<const column_definition*> _columns;
    ::shared_ptr<metadata> _metadata;
    const bool _collect_timestamps;
    const bool _collect_TTLs;
protected:
    selection(schema_ptr schema, std::vector<const column_definition*> columns, std::vector<::shared_ptr<column_specification>> metadata_,
            bool collect_timestamps, bool collect_TTLs)
        : _schema(std::move(schema))
        , _columns(std::move(columns))
        , _metadata(::make_shared<metadata>(std::move(metadata_)))
        , _collect_timestamps(collect_timestamps)
        , _collect_TTLs(collect_TTLs)
    { }
    virtual ~selection() {}
public:
    // Overriden by SimpleSelection when appropriate.
    virtual bool is_wildcard() const {
        return false;
    }

    /**
     * Checks if this selection contains static columns.
     * @return <code>true</code> if this selection contains static columns, <code>false</code> otherwise;
     */
    bool contains_static_columns() const {
        if (!_schema->has_static_columns()) {
            return false;
        }

        if (is_wildcard()) {
            return true;
        }

        return std::any_of(_columns.begin(), _columns.end(), [] (auto&& def) { return def->is_static(); });
    }

    /**
     * Checks if this selection contains only static columns.
     * @return <code>true</code> if this selection contains only static columns, <code>false</code> otherwise;
     */
    bool contains_only_static_columns() const {
        if (!contains_static_columns()) {
            return false;
        }

        if (is_wildcard()) {
            return false;
        }

        for (auto&& def : _columns) {
            if (!def->is_partition_key() && !def->is_static()) {
                return false;
            }
        }

        return true;
    }

    /**
     * Checks if this selection contains a collection.
     *
     * @return <code>true</code> if this selection contains a collection, <code>false</code> otherwise.
     */
    bool contains_a_collection() const {
        if (!_schema->has_collections()) {
            return false;
        }

        return std::any_of(_columns.begin(), _columns.end(), [] (auto&& def) {
           return def->type->is_collection() && def->type->is_multi_cell();
        });
    }

    /**
     * Returns the index of the specified column.
     *
     * @param def the column definition
     * @return the index of the specified column
     */
    int32_t index_of(const column_definition& def) const {
        auto i = std::find(_columns.begin(), _columns.end(), &def);
        if (i == _columns.end()) {
            return -1;
        }
        return std::distance(_columns.begin(), i);
    }

    bool has_column(const column_definition& def) const {
        return std::find(_columns.begin(), _columns.end(), &def) != _columns.end();
    }

    ::shared_ptr<metadata> get_result_metadata() {
        return _metadata;
    }

    static ::shared_ptr<selection> wildcard(schema_ptr schema);
    static ::shared_ptr<selection> for_columns(schema_ptr schema, std::vector<const column_definition*> columns);

    virtual uint32_t add_column_for_ordering(const column_definition& c);

    virtual bool uses_function(const sstring &ks_name, const sstring& function_name) const {
        return false;
    }
private:
    static bool processes_selection(const std::vector<::shared_ptr<raw_selector>>& raw_selectors) {
        return std::any_of(raw_selectors.begin(), raw_selectors.end(),
            [] (auto&& s) { return s->processes_selection(); });
    }

    static std::vector<::shared_ptr<column_specification>> collect_metadata(schema_ptr schema,
        const std::vector<::shared_ptr<raw_selector>>& raw_selectors, const selector_factories& factories);
public:
    static ::shared_ptr<selection> from_selectors(schema_ptr schema, const std::vector<::shared_ptr<raw_selector>>& raw_selectors);

    virtual std::unique_ptr<selectors> new_selectors() = 0;

    /**
     * Returns a range of CQL3 columns this selection needs.
     */
    auto const& get_columns() {
        return _columns;
    }

    uint32_t get_column_count() {
        return _columns.size();
    }

    ::shared_ptr<result_set_builder> make_result_set_builder(db_clock::time_point now, serialization_format sf) {
        return ::make_shared<result_set_builder>(*this, now, sf);
    }

    virtual bool is_aggregate() const = 0;

    /**
     * Checks that selectors are either all aggregates or that none of them is.
     *
     * @param selectors the selectors to test.
     * @param messageTemplate the error message template
     * @param messageArgs the error message arguments
     * @throws InvalidRequestException if some of the selectors are aggregate but not all of them
     */
    template<typename... Args>
    static void validate_selectors(const std::vector<::shared_ptr<selector>>& selectors, const sstring& msg, Args&&... args) {
        int32_t aggregates = 0;
        for (auto&& s : selectors) {
            if (s->is_aggregate()) {
                ++aggregates;
            }
        }

        if (aggregates != 0 && aggregates != selectors.size()) {
            throw exceptions::invalid_request_exception(sprint(msg, std::forward<Args>(args)...));
        }
    }

    friend class result_set_builder;
};

class result_set_builder {
private:
    std::unique_ptr<result_set> _result_set;
    std::unique_ptr<selectors> _selectors;
public:
    std::experimental::optional<std::vector<bytes_opt>> current;
private:
    std::vector<api::timestamp_type> _timestamps;
    std::vector<int32_t> _ttls;
    const db_clock::time_point _now;
    serialization_format _serialization_format;
public:
    result_set_builder(selection& s, db_clock::time_point now, serialization_format sf);

    void add_empty() {
        current->emplace_back();
    }

    void add(bytes_opt value) {
        current->emplace_back(std::move(value));
    }

    void add(const column_definition& def, atomic_cell_view c) {
        current->emplace_back(get_value(def.type, c));
        if (!_timestamps.empty()) {
            _timestamps[current->size() - 1] = c.is_dead() ? api::min_timestamp : c.timestamp();
        }
        if (!_ttls.empty()) {
            gc_clock::duration ttl(-1);
            if (c.is_live_and_has_ttl()) {
                ttl = *c.ttl() - to_gc_clock(_now);
            }
            _ttls[current->size() - 1] = ttl.count();
        }
    }

    void add(const column_definition& def, collection_mutation::view c) {
        auto&& ctype = static_cast<collection_type_impl*>(def.type.get());
        current->emplace_back(ctype->to_value(c, _serialization_format));
        // timestamps, ttls meaningless for collections
    }

    void new_row() {
        if (current) {
            _selectors->add_input_row(_serialization_format, *this);
            if (!_selectors->is_aggregate()) {
                _result_set->add_row(_selectors->get_output_row(_serialization_format));
                _selectors->reset();
            }
            current->clear();
        } else {
            // FIXME: we use optional<> here because we don't have an end_row() signal
            //        instead, !current means that new_row has never been called, so this
            //        call to new_row() does not end a previous row.
            current.emplace();
        }
    }

    std::unique_ptr<result_set> build() {
        if (current) {
            _selectors->add_input_row(_serialization_format, *this);
            _result_set->add_row(_selectors->get_output_row(_serialization_format));
            _selectors->reset();
            current = std::experimental::nullopt;
        }
        if (_result_set->empty() && _selectors->is_aggregate()) {
            _result_set->add_row(_selectors->get_output_row(_serialization_format));
        }
        return std::move(_result_set);
    }

private:
    bytes_opt get_value(data_type t, atomic_cell_view c) {
        if (c.is_dead()) {
            return {};
        }
        if (t->is_counter()) {
            fail(unimplemented::cause::COUNTERS);
#if 0
                ByteBufferUtil.bytes(CounterContext.instance().total(c.value()))
#endif
        }
        return {to_bytes(c.value())};
    }
};

}

}
