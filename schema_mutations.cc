/*
 * Copyright 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "schema_mutations.hh"
#include "mutation/canonical_mutation.hh"
#include "db/schema_tables.hh"
#include "utils/hashers.hh"
#include "utils/UUID_gen.hh"

schema_mutations::schema_mutations(canonical_mutation columnfamilies,
                                   canonical_mutation columns,
                                   bool is_view,
                                   std::optional<canonical_mutation> indices,
                                   std::optional<canonical_mutation> dropped_columns,
                                   std::optional<canonical_mutation> scylla_tables,
                                   std::optional<canonical_mutation> view_virtual_columns,
                                   std::optional<canonical_mutation> computed_columns)
    : _columnfamilies(columnfamilies.to_mutation(is_view ? db::schema_tables::views() : db::schema_tables::tables()))
    , _columns(columns.to_mutation(db::schema_tables::columns()))
    , _view_virtual_columns(view_virtual_columns ? mutation_opt{view_virtual_columns.value().to_mutation(db::schema_tables::view_virtual_columns())} : std::nullopt)
    , _computed_columns(computed_columns ? mutation_opt{computed_columns.value().to_mutation(db::schema_tables::computed_columns())} : std::nullopt)
    , _indices(indices ? mutation_opt{indices.value().to_mutation(db::schema_tables::indexes())} : std::nullopt)
    , _dropped_columns(dropped_columns ? mutation_opt{dropped_columns.value().to_mutation(db::schema_tables::dropped_columns())} : std::nullopt)
    , _scylla_tables(scylla_tables ? mutation_opt{scylla_tables.value().to_mutation(db::schema_tables::scylla_tables())} : std::nullopt)
{}

void schema_mutations::copy_to(std::vector<mutation>& dst) const {
    dst.push_back(_columnfamilies);
    dst.push_back(_columns);
    if (_view_virtual_columns) {
        dst.push_back(*_view_virtual_columns);
    }
    if (_computed_columns) {
        dst.push_back(*_computed_columns);
    }
    if (_indices) {
        dst.push_back(*_indices);
    }
    if (_dropped_columns) {
        dst.push_back(*_dropped_columns);
    }
    if (_scylla_tables) {
        dst.push_back(*_scylla_tables);
    }
}

table_schema_version schema_mutations::digest(db::schema_features sf) const {
    if (_scylla_tables) {
        auto rs = query::result_set(*_scylla_tables);
        if (!rs.empty()) {
            auto&& row = rs.row(0);
            auto val = row.get<utils::UUID>("version");
            if (val) {
                return table_schema_version(*val);
            }
        }
    }

    md5_hasher h;

    if (!sf.contains<db::schema_feature::TABLE_DIGEST_INSENSITIVE_TO_EXPIRY>()) {
        // Disable this feature so that the digest remains compactible with Scylla
        // versions prior to this feature.
        // This digest affects the table schema version calculation and it's important
        // that all nodes arrive at the same table schema version to avoid needless schema version
        // pulls. It used to be the case that when table schema versions were calculated on boot we
        // didn't yet know all the cluster features, so we could get different table versions after reboot
        // in an already upgraded cluster. However, they are now available, and if
        // TABLE_DIGEST_INSENSITIVE_TO_EXPIRY is enabled, we can compute with DIGEST_INSENSITIVE_TO_EXPIRY
        // enabled.
        sf.remove<db::schema_feature::DIGEST_INSENSITIVE_TO_EXPIRY>();
    }

    db::schema_tables::feed_hash_for_schema_digest(h, _columnfamilies, sf);
    db::schema_tables::feed_hash_for_schema_digest(h, _columns, sf);
    if (_view_virtual_columns && !_view_virtual_columns->partition().empty()) {
        db::schema_tables::feed_hash_for_schema_digest(h, *_view_virtual_columns, sf);
    }
    if (_computed_columns && !_computed_columns->partition().empty()) {
        db::schema_tables::feed_hash_for_schema_digest(h, *_computed_columns, sf);
    }
    if (_indices && !_indices->partition().empty()) {
        db::schema_tables::feed_hash_for_schema_digest(h, *_indices, sf);
    }
    if (_dropped_columns && !_dropped_columns->partition().empty()) {
        db::schema_tables::feed_hash_for_schema_digest(h, *_dropped_columns, sf);
    }
    if (_scylla_tables) {
        db::schema_tables::feed_hash_for_schema_digest(h, *_scylla_tables, sf);
    }
    return table_schema_version(utils::UUID_gen::get_name_UUID(h.finalize()));
}

std::optional<sstring> schema_mutations::partitioner() const {
    if (_scylla_tables) {
        auto rs = query::result_set(*_scylla_tables);
        if (!rs.empty()) {
            return rs.row(0).get<sstring>("partitioner");
        }
    }
    return { };
}

static mutation_opt compact(const mutation_opt& m) {
    if (!m) {
        return m;
    }
    return db::schema_tables::compact_for_schema_digest(*m);
}

static mutation_opt compact(const mutation& m) {
    return db::schema_tables::compact_for_schema_digest(m);
}

bool schema_mutations::operator==(const schema_mutations& other) const {
    return compact(_columnfamilies) == compact(other._columnfamilies)
           && compact(_columns) == compact(other._columns)
           && compact(_view_virtual_columns) == compact(other._view_virtual_columns)
           && compact(_computed_columns) == compact(other._computed_columns)
           && compact(_indices) == compact(other._indices)
           && compact(_dropped_columns) == compact(other._dropped_columns)
           && compact(_scylla_tables) == compact(other._scylla_tables)
           ;
}

bool schema_mutations::live() const {
    return _columnfamilies.live_row_count() > 0 || _columns.live_row_count() > 0 ||
            (_view_virtual_columns && _view_virtual_columns->live_row_count() > 0) ||
            (_computed_columns && _computed_columns->live_row_count() > 0);
}

bool schema_mutations::is_view() const {
    return _columnfamilies.schema() == db::schema_tables::views();
}

auto fmt::formatter<schema_mutations>::format(const schema_mutations& sm, fmt::format_context& ctx) const
        -> decltype(ctx.out()) {
    auto out = fmt::format_to(ctx.out(), "schema_mutations{{\n");
    out = fmt::format_to(out, " tables={},\n", sm.columnfamilies_mutation());
    out = fmt::format_to(out, " scylla_tables={},\n", sm.scylla_tables());
    out = fmt::format_to(out, " tables={},\n", sm.columns_mutation());
    out = fmt::format_to(out, " dropped_columns={},\n", sm.dropped_columns_mutation());
    out = fmt::format_to(out, " indices={},\n", sm.indices_mutation());
    out = fmt::format_to(out, " computed_columns={},\n", sm.computed_columns_mutation());
    out = fmt::format_to(out, " view_virtual_columns={},\n", sm.view_virtual_columns_mutation());
    return fmt::format_to(out, "}}");
}

schema_mutations& schema_mutations::operator+=(schema_mutations&& sm) {
    _columnfamilies += std::move(sm._columnfamilies);
    _columns += std::move(sm._columns);
    apply(_computed_columns, std::move(sm._computed_columns));
    apply(_view_virtual_columns, std::move(sm._view_virtual_columns));
    apply(_indices, std::move(sm._indices));
    apply(_dropped_columns, std::move(sm._dropped_columns));
    apply(_scylla_tables, std::move(sm._scylla_tables));
    return *this;
}
