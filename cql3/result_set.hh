/*
 * Copyright (C) 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

#include <vector>
#include "utils/chunked_vector.hh"
#include "enum_set.hh"
#include "service/pager/paging_state.hh"

#include "result_generator.hh"


namespace cql3 {

class metadata {
public:
    enum class flag : uint8_t {
        GLOBAL_TABLES_SPEC = 0,
        HAS_MORE_PAGES = 1,
        NO_METADATA = 2,
    };

    using flag_enum = super_enum<flag,
        flag::GLOBAL_TABLES_SPEC,
        flag::HAS_MORE_PAGES,
        flag::NO_METADATA>;

    using flag_enum_set = enum_set<flag_enum>;

    struct column_info {
    // Please note that columnCount can actually be smaller than names, even if names is not null. This is
    // used to include columns in the resultSet that we need to do post-query re-orderings
    // (SelectStatement.orderResults) but that shouldn't be sent to the user as they haven't been requested
    // (CASSANDRA-4911). So the serialization code will exclude any columns in name whose index is >= columnCount.
        std::vector<lw_shared_ptr<column_specification>> _names;
        uint32_t _column_count;

        column_info(std::vector<lw_shared_ptr<column_specification>> names, uint32_t column_count)
            : _names(std::move(names))
            , _column_count(column_count)
        { }

        explicit column_info(std::vector<lw_shared_ptr<column_specification>> names)
            : _names(std::move(names))
            , _column_count(_names.size())
        { }
    };
private:
    flag_enum_set _flags;

private:
    lw_shared_ptr<column_info> _column_info;
    lw_shared_ptr<const service::pager::paging_state> _paging_state;

public:
    metadata(std::vector<lw_shared_ptr<column_specification>> names_);

    metadata(flag_enum_set flags, std::vector<lw_shared_ptr<column_specification>> names_, uint32_t column_count,
            lw_shared_ptr<const service::pager::paging_state> paging_state);

    // The maximum number of values that the ResultSet can hold. This can be bigger than columnCount due to CASSANDRA-4911
    uint32_t value_count() const;

    void add_non_serialized_column(lw_shared_ptr<column_specification> name);

private:
    bool all_in_same_cf() const;

public:
    void set_paging_state(lw_shared_ptr<const service::pager::paging_state> paging_state);
    void maybe_set_paging_state(lw_shared_ptr<const service::pager::paging_state> paging_state);

    void set_skip_metadata();

    flag_enum_set flags() const;

    uint32_t column_count() const { return _column_info->_column_count; }

    lw_shared_ptr<const service::pager::paging_state> paging_state() const;

    const std::vector<lw_shared_ptr<column_specification>>& get_names() const {
        return _column_info->_names;
    }
};

::shared_ptr<const cql3::metadata> make_empty_metadata();

class prepared_metadata {
public:
    enum class flag : uint32_t {
        GLOBAL_TABLES_SPEC = 0,
        // Denotes whether the prepared statement at hand is an LWT statement.
        //
        // Use the last available bit in the flags since we don't want to clash
        // with C* in case they add some other flag in one the next versions of binary protocol.
        LWT = 31
    };

    using flag_enum = super_enum<flag,
        flag::GLOBAL_TABLES_SPEC,
        flag::LWT>;

    using flag_enum_set = enum_set<flag_enum>;

    static constexpr flag_enum_set::mask_type LWT_FLAG_MASK = flag_enum_set::mask_for<flag::LWT>();

private:
    flag_enum_set _flags;
    std::vector<lw_shared_ptr<column_specification>> _names;
    std::vector<uint16_t> _partition_key_bind_indices;
public:
    prepared_metadata(const std::vector<lw_shared_ptr<column_specification>>& names,
                      const std::vector<uint16_t>& partition_key_bind_indices,
                      bool is_conditional);

    flag_enum_set flags() const;
    const std::vector<lw_shared_ptr<column_specification>>& names() const;
    const std::vector<uint16_t>& partition_key_bind_indices() const;
};

template<typename Visitor>
concept ResultVisitor = requires(Visitor& visitor, managed_bytes_view_opt val) {
    visitor.start_row();
    visitor.accept_value(std::move(val));
    visitor.end_row();
};

class result_set {
    using col_type = managed_bytes_opt;
    using row_type = std::vector<col_type>;
    using rows_type = utils::chunked_vector<row_type>;

    ::shared_ptr<metadata> _metadata;
    rows_type _rows;

    friend class result;
public:
    result_set(std::vector<lw_shared_ptr<column_specification>> metadata_);

    result_set(::shared_ptr<metadata> metadata);

    result_set(result_set&& other) = default;
    result_set(const result_set& other) = delete;

    size_t size() const;

    bool empty() const;

    void add_row(row_type row);
    void add_row(std::vector<bytes_opt> row);

    void add_column_value(col_type value);
    void add_column_value(bytes_opt value);

    void reverse();

    void trim(size_t limit);

    template<typename RowComparator>
    requires requires (RowComparator cmp, const row_type& row) {
        { cmp(row, row) } -> std::same_as<bool>;
    }
    void sort(const RowComparator& cmp) {
        std::sort(_rows.begin(), _rows.end(), cmp);
    }

    metadata& get_metadata();

    const metadata& get_metadata() const;

    // Returns a range of rows. A row is a range of bytes_opt.
    const rows_type& rows() const;

    template<typename Visitor>
    requires ResultVisitor<Visitor>
    void visit(Visitor&& visitor) const {
        auto column_count = get_metadata().column_count();
        for (auto& row : _rows) {
            visitor.start_row();
            for (auto i = 0u; i < column_count; i++) {
                auto& cell = row[i];
                visitor.accept_value(cell ? managed_bytes_view_opt(*cell) : managed_bytes_view_opt());
            }
            visitor.end_row();
        }
    }

    // visit_gently() is like visit(), except it may yield between rows and
    // returns a future that will resolve when it's done. We only yield
    // between rows, not between individual cells, which is a good compromise
    // if we assume that individual rows are not too large.
    future<> visit_gently(ResultVisitor auto& visitor) const {
        auto column_count = get_metadata().column_count();
        return do_for_each(_rows, [&visitor, column_count] (auto& row) {
            visitor.start_row();
            for (auto i = 0u; i < column_count; i++) {
                auto& cell = row[i];
                visitor.accept_value(cell ? managed_bytes_view_opt(*cell) : managed_bytes_view_opt());
            }
            visitor.end_row();
        });
    }


    class builder;
};

class result_set::builder {
    result_set _result;
    row_type _current_row;
public:
    explicit builder(shared_ptr<metadata> mtd)
        : _result(std::move(mtd)) { }

    void start_row() { }
    void accept_value(managed_bytes_view_opt value) {
        if (!value) {
            _current_row.emplace_back();
            return;
        }
        _current_row.emplace_back(value);
    }
    void end_row() {
        _result.add_row(std::exchange(_current_row, { }));
    }
    result_set get_result_set() && { return std::move(_result); }
};

class result {
    mutable std::unique_ptr<cql3::result_set> _result_set;
    result_generator _result_generator;
    shared_ptr<const cql3::metadata> _metadata;
public:
    explicit result(std::unique_ptr<cql3::result_set> rs)
        : _result_set(std::move(rs))
        , _metadata(_result_set->_metadata)
    { }

    explicit result(result_generator generator, shared_ptr<const metadata> m)
        : _result_generator(std::move(generator))
        , _metadata(std::move(m))
    { }

    const cql3::metadata& get_metadata() const { return *_metadata; }
    const cql3::result_set& result_set() const {
        if (_result_set) {
            return *_result_set;
        }
        auto builder = result_set::builder(make_shared<cql3::metadata>(*_metadata));
        _result_generator.visit(builder);
        auto tmp_rs = std::make_unique<cql3::result_set>(std::move(builder).get_result_set());
        _result_set.swap(tmp_rs);
        return *_result_set;
    }

    template<typename Visitor>
    requires ResultVisitor<Visitor>
    void visit(Visitor&& visitor) const {
        if (_result_set) {
            _result_set->visit(std::forward<Visitor>(visitor));
        } else {
            _result_generator.visit(std::forward<Visitor>(visitor));
        }
    }
};

}
