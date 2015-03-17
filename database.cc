/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 */

#include "log.hh"
#include "database.hh"
#include "unimplemented.hh"
#include "core/future-util.hh"
#include "db/system_keyspace.hh"

#include "cql3/column_identifier.hh"
#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/split.hpp>

thread_local logging::logger dblog("database");

template<typename Sequence>
std::vector<::shared_ptr<abstract_type>>
get_column_types(const Sequence& column_definitions) {
    std::vector<shared_ptr<abstract_type>> result;
    for (auto&& col : column_definitions) {
        result.push_back(col.type);
    }
    return result;
}

::shared_ptr<cql3::column_specification>
schema::make_column_specification(column_definition& def) {
    auto id = ::make_shared<cql3::column_identifier>(def.name(), column_name_type(def));
    return ::make_shared<cql3::column_specification>(ks_name, cf_name, std::move(id), def.type);
}

void
schema::build_columns(const std::vector<column>& columns, column_definition::column_kind kind,
    std::vector<column_definition>& dst)
{
    dst.reserve(columns.size());
    for (column_id i = 0; i < columns.size(); i++) {
        auto& col = columns[i];
        dst.emplace_back(std::move(col.name), std::move(col.type), i, kind);
        column_definition& def = dst.back();
        _columns_by_name[def.name()] = &def;
        def.column_specification = make_column_specification(def);
    }
}

schema::schema(sstring ks_name, sstring cf_name, std::vector<column> partition_key,
    std::vector<column> clustering_key,
    std::vector<column> regular_columns,
    std::vector<column> static_columns,
    data_type regular_column_name_type,
    sstring comment)
        : _regular_columns_by_name(serialized_compare(regular_column_name_type))
        , _comment(comment)
        , ks_name(std::move(ks_name))
        , cf_name(std::move(cf_name))
        , partition_key_type(::make_shared<tuple_type<>>(get_column_types(partition_key)))
        , clustering_key_type(::make_shared<tuple_type<>>(get_column_types(clustering_key)))
        , clustering_key_prefix_type(::make_shared(clustering_key_type->as_prefix()))
        , partition_key_prefix_type(::make_shared(partition_key_type->as_prefix()))
        , regular_column_name_type(regular_column_name_type)
{
    if (partition_key.size() == 1) {
        thrift.partition_key_type = partition_key[0].type;
    } else {
        // TODO: the type should be composite_type
        warn(unimplemented::cause::LEGACY_COMPOSITE_KEYS);
    }

    build_columns(partition_key, column_definition::column_kind::PARTITION, _partition_key);
    build_columns(clustering_key, column_definition::column_kind::CLUSTERING, _clustering_key);

    std::sort(regular_columns.begin(), regular_columns.end(), column::name_compare(regular_column_name_type));
    build_columns(regular_columns, column_definition::column_kind::REGULAR, _regular_columns);
    for (column_definition& def : _regular_columns) {
        _regular_columns_by_name[def.name()] = &def;
    }

    std::sort(static_columns.begin(), static_columns.end(), column::name_compare(utf8_type));
    build_columns(static_columns, column_definition::column_kind::STATIC, _static_columns);
}

column_family::column_family(schema_ptr schema)
    : _schema(std::move(schema))
    , partitions(partition_key::one::less_compare(*_schema)) {
}

mutation_partition*
column_family::find_partition(const partition_key::one& key) {
    auto i = partitions.find(key);
    return i == partitions.end() ? nullptr : &i->second;
}

row*
column_family::find_row(const partition_key::one& partition_key, const clustering_key::one& clustering_key) {
    mutation_partition* p = find_partition(partition_key);
    if (!p) {
        return nullptr;
    }
    return p->find_row(clustering_key);
}

mutation_partition&
column_family::find_or_create_partition(const partition_key::one& key) {
    // call lower_bound so we have a hint for the insert, just in case.
    auto i = partitions.lower_bound(key);
    if (i == partitions.end() || !key.equal(*_schema, i->first)) {
        i = partitions.emplace_hint(i, std::make_pair(std::move(key), mutation_partition(_schema)));
    }
    return i->second;
}

row&
column_family::find_or_create_row(const partition_key::one& partition_key, const clustering_key::one& clustering_key) {
    mutation_partition& p = find_or_create_partition(partition_key);
    return p.clustered_row(clustering_key);
}

static inline int8_t hex_to_int(unsigned char c) {
    switch (c) {
    case '0': return 0;
    case '1': return 1;
    case '2': return 2;
    case '3': return 3;
    case '4': return 4;
    case '5': return 5;
    case '6': return 6;
    case '7': return 7;
    case '8': return 8;
    case '9': return 9;
    case 'a': case 'A': return 10;
    case 'b': case 'B': return 11;
    case 'c': case 'C': return 12;
    case 'd': case 'D': return 13;
    case 'e': case 'E': return 14;
    case 'f': case 'F': return 15;
    default:
        return -1;
    }
}

bytes from_hex(sstring_view s) {
    if (s.length() % 2 == 1) {
        throw std::invalid_argument("An hex string representing bytes must have an even length");
    }
    bytes out{bytes::initialized_later(), s.length() / 2};
    unsigned end = out.size();
    for (unsigned i = 0; i != end; i++) {
        auto half_byte1 = hex_to_int(s[i * 2]);
        auto half_byte2 = hex_to_int(s[i * 2 + 1]);
        if (half_byte1 == -1 || half_byte2 == -1) {
            throw std::invalid_argument(sprint("Non-hex characters in %s", s));
        }
        out[i] = (half_byte1 << 4) | half_byte2;
    }
    return out;
}

sstring to_hex(bytes_view b) {
    static char digits[] = "0123456789abcdef";
    sstring out(sstring::initialized_later(), b.size() * 2);
    unsigned end = b.size();
    for (unsigned i = 0; i != end; ++i) {
        uint8_t x = b[i];
        out[2*i] = digits[x >> 4];
        out[2*i+1] = digits[x & 0xf];
    }
    return out;
}

sstring to_hex(const bytes& b) {
    return to_hex(bytes_view(b));
}

sstring to_hex(const bytes_opt& b) {
    return b ? "null" : to_hex(*b);
}

class lister {
    file _f;
    std::function<future<> (directory_entry de)> _walker;
    directory_entry_type _expected_type;
    subscription<directory_entry> _listing;

public:
    lister(file f, directory_entry_type type, std::function<future<> (directory_entry)> walker)
            : _f(std::move(f))
            , _walker(std::move(walker))
            , _expected_type(type)
            , _listing(_f.list_directory([this] (directory_entry de) { return _visit(de); })) {
    }

    static future<> scan_dir(sstring name, directory_entry_type type, std::function<future<> (directory_entry)> walker);
protected:
    future<> _visit(directory_entry de) {

        // FIXME: stat and try to recover
        if (!de.type) {
            dblog.error("database found file with unknown type {}", de.name);
            return make_ready_future<>();
        }

        // Hide all synthetic directories and hidden files.
        if ((de.type != _expected_type) || (de.name[0] == '.')) {
            return make_ready_future<>();
        }
        return _walker(de);
    }
    future<> done() { return _listing.done(); }
};


future<> lister::scan_dir(sstring name, directory_entry_type type, std::function<future<> (directory_entry)> walker) {

    return engine().open_directory(name).then([type, walker = std::move(walker)] (file f) {
        auto l = make_lw_shared<lister>(std::move(f), type, walker);
        return l->done().then([l] { });
    });
}

static std::vector<sstring> parse_fname(sstring filename) {
    std::vector<sstring> comps;
    boost::split(comps , filename ,boost::is_any_of(".-"));
    return comps;
}

future<> keyspace::populate(sstring ksdir) {
    return lister::scan_dir(ksdir, directory_entry_type::directory, [this, ksdir] (directory_entry de) {
        auto comps = parse_fname(de.name);
        if (comps.size() != 2) {
            dblog.error("Keyspace {}: Skipping malformed CF {} ", ksdir, de.name);
            return make_ready_future<>();
        }
        sstring cfname = comps[0];

        auto sstdir = ksdir + "/" + de.name;
        dblog.warn("Keyspace {}: Reading CF {} ", ksdir, comps[0]);
        return make_ready_future<>();
    });
}

future<> database::populate(sstring datadir) {
    return lister::scan_dir(datadir, directory_entry_type::directory, [this, datadir] (directory_entry de) {
        auto& ks_name = de.name;
        auto ksdir = datadir + "/" + de.name;

        auto i = keyspaces.find(ks_name);
        if (i == keyspaces.end()) {
            dblog.warn("Skipping undefined keyspace: {}", ks_name);
        } else {
            dblog.warn("Populating Keyspace {}", ks_name);
            return i->second.populate(ksdir);
        }
        return make_ready_future<>();
    });
}

future<>
database::init_from_data_directory(sstring datadir) {
    keyspaces.emplace("system", db::system_keyspace::make());
    return populate(datadir);
}

unsigned
database::shard_of(const dht::token& t) {
    if (t._data.empty()) {
        return 0;
    }
    return uint8_t(t._data[0]) % smp::count;
}

column_definition::column_definition(bytes name, data_type type, column_id id, column_kind kind)
    : _name(std::move(name))
    , type(std::move(type))
    , id(id)
    , kind(kind)
{ }

column_definition* schema::get_column_definition(const bytes& name) {
    auto i = _columns_by_name.find(name);
    if (i == _columns_by_name.end()) {
        return nullptr;
    }
    return i->second;
}

const sstring&
column_definition::name_as_text() const {
    return column_specification->name->text();
}

const bytes&
column_definition::name() const {
    return _name;
}

column_family*
keyspace::find_column_family(const sstring& cf_name) {
    auto i = column_families.find(cf_name);
    if (i == column_families.end()) {
        return nullptr;
    }
    return &i->second;
}

schema_ptr
keyspace::find_schema(const sstring& cf_name) {
    auto cf = find_column_family(cf_name);
    if (!cf) {
        return {};
    }
    return cf->_schema;
}

schema_ptr database::find_schema(const sstring& ks_name, const sstring& cf_name) {
    auto ks = find_keyspace(ks_name);
    if (!ks) {
        return {};
    }

    return ks->find_schema(cf_name);
}

keyspace*
database::find_keyspace(const sstring& name) {
    auto i = keyspaces.find(name);
    if (i != keyspaces.end()) {
        return &i->second;
    }
    return nullptr;
}

void
column_family::apply(const mutation& m) {
    mutation_partition& p = find_or_create_partition(m.key);
    p.apply(_schema, m.p);
}

// Based on org.apache.cassandra.db.AbstractCell#reconcile()
int
compare_atomic_cell_for_merge(atomic_cell::view left, atomic_cell::view right) {
    if (left.timestamp() != right.timestamp()) {
        return left.timestamp() > right.timestamp() ? 1 : -1;
    }
    if (left.is_live() != right.is_live()) {
        return left.is_live() ? -1 : 1;
    }
    if (left.is_live()) {
        return compare_unsigned(left.value(), right.value());
    } else {
        if (*left.ttl() != *right.ttl()) {
            // Origin compares big-endian serialized TTL
            return (uint32_t)left.ttl()->time_since_epoch().count()
                 < (uint32_t)right.ttl()->time_since_epoch().count() ? -1 : 1;
        }
        return 0;
    }
}

void
merge_column(const column_definition& def,
             atomic_cell_or_collection& old,
             const atomic_cell_or_collection& neww) {
    if (def.is_atomic()) {
        if (compare_atomic_cell_for_merge(old.as_atomic_cell(), neww.as_atomic_cell()) < 0) {
            // FIXME: move()?
            old = neww;
        }
    } else {
        auto ct = static_pointer_cast<collection_type_impl>(def.type);
        old = ct->merge(old.as_collection_mutation(), neww.as_collection_mutation());
    }
}

mutation_partition::~mutation_partition() {
    _rows.clear_and_dispose(std::default_delete<rows_entry>());
    _row_tombstones.clear_and_dispose(std::default_delete<row_tombstones_entry>());
}

void
mutation_partition::apply(schema_ptr schema, const mutation_partition& p) {
    _tombstone.apply(p._tombstone);

    for (auto&& e : p._row_tombstones) {
        apply_row_tombstone(schema, e.prefix(), e.t());
    }

    auto merge_cells = [this, schema] (row& old_row, const row& new_row, auto&& find_column_def) {
        for (auto&& new_column : new_row) {
            auto col = new_column.first;
            auto i = old_row.find(col);
            if (i == old_row.end()) {
                old_row.emplace_hint(i, new_column);
            } else {
                auto& old_column = *i;
                auto& def = find_column_def(col);
                merge_column(def, old_column.second, new_column.second);
            }
        }
    };

    auto find_static_column_def = [schema] (auto col) -> column_definition& { return schema->static_column_at(col); };
    auto find_regular_column_def = [schema] (auto col) -> column_definition& { return schema->regular_column_at(col); };

    merge_cells(_static_row, p._static_row, find_static_column_def);

    for (auto&& entry : p._rows) {
        auto& key = entry.key();
        auto i = _rows.find(key, rows_entry::compare(*schema));
        if (i == _rows.end()) {
            auto e = new rows_entry(entry);
            _rows.insert(i, *e);
        } else {
            i->apply(entry.row().t);
            merge_cells(i->row().cells, entry.row().cells, find_regular_column_def);
        }
    }
}

tombstone
mutation_partition::tombstone_for_row(schema_ptr schema, const clustering_key::one& key) {
    tombstone t = _tombstone;

    auto c = row_tombstones_entry::key_comparator(
        clustering_key::one::prefix_view_type::less_compare_with_prefix(*schema));

    // _row_tombstones contains only strict prefixes
    for (unsigned prefix_len = 1; prefix_len < schema->clustering_key_size(); ++prefix_len) {
        auto i = _row_tombstones.find(key.prefix_view(*schema, prefix_len), c);
        if (i != _row_tombstones.end()) {
            t.apply(i->t());
        }
    }

    auto j = _rows.find(key, rows_entry::compare(*schema));
    if (j != _rows.end()) {
        t.apply(j->row().t);
    }

    return t;
}

void
mutation_partition::apply_row_tombstone(schema_ptr schema, clustering_key::prefix::one prefix, tombstone t) {
    assert(!prefix.is_full(*schema));
    auto i = _row_tombstones.lower_bound(prefix, row_tombstones_entry::compare(*schema));
    if (i == _row_tombstones.end() || !prefix.equal(*schema, i->prefix())) {
        auto e = new row_tombstones_entry(std::move(prefix), t);
        _row_tombstones.insert(i, *e);
    } else {
        i->apply(t);
    }
}

void
mutation_partition::apply_delete(schema_ptr schema, const clustering_prefix& prefix, tombstone t) {
    if (!prefix) {
        apply(t);
    } else if (prefix.is_full(*schema)) {
        apply_delete(schema, clustering_key::one::from_clustering_prefix(*schema, prefix), t);
    } else {
        apply_row_tombstone(schema, clustering_key::prefix::one::from_clustering_prefix(*schema, prefix), t);
    }
}

void
mutation_partition::apply_delete(schema_ptr schema, clustering_key::one&& key, tombstone t) {
    auto i = _rows.lower_bound(key, rows_entry::compare(*schema));
    if (i == _rows.end() || !i->key().equal(*schema, key)) {
        auto e = new rows_entry(std::move(key));
        e->row().apply(t);
        _rows.insert(i, *e);
    } else {
        i->row().apply(t);
    }
}

rows_entry*
mutation_partition::find_entry(schema_ptr schema, const clustering_key::prefix::one& key) {
    auto i = _rows.find(key, rows_entry::compare_prefix(*schema));
    if (i == _rows.end()) {
        return nullptr;
    }
    return &*i;
}

row*
mutation_partition::find_row(const clustering_key::one& key) {
    auto i = _rows.find(key);
    if (i == _rows.end()) {
        return nullptr;
    }
    return &i->row().cells;
}

row&
mutation_partition::clustered_row(const clustering_key::one& key) {
    auto i = _rows.find(key);
    if (i == _rows.end()) {
        auto e = new rows_entry(key);
        _rows.insert(i, *e);
        return e->row().cells;
    }
    return i->row().cells;
}

bool column_definition::is_compact_value() const {
    warn(unimplemented::cause::COMPACT_TABLES);
    return false;
}

std::ostream& operator<<(std::ostream& os, const mutation& m) {
    return fprint(os, "{mutation: schema %p key %s data %s}", m.schema.get(), static_cast<bytes_view>(m.key), m.p);
}

std::ostream& operator<<(std::ostream& os, const mutation_partition& mp) {
    return fprint(os, "{mutation_partition: ...}");
}

query::result::partition
column_family::get_partition_slice(mutation_partition& partition, const query::partition_slice& slice, uint32_t limit) {
    query::result::partition result;

    for (auto&& range : slice.row_ranges) {
        if (limit == 0) {
            break;
        }
        if (!range.is_singular()) {
            fail(unimplemented::cause::RANGE_QUERIES);
        }
        auto& key = range.start_value();
        if (!key.is_full(*_schema)) {
            fail(unimplemented::cause::RANGE_QUERIES);
        }

        rows_entry* row = partition.find_entry(_schema, key);
        if (!row) {
            continue;
        }

        auto&& cells = &row->row().cells;

        // FIXME: handle removed rows properly. In CQL rows are separate entities (can be live or dead).
        auto row_tombstone = partition.tombstone_for_row(_schema, row->key());

        query::result::row result_row;
        result_row.cells.reserve(slice.regular_columns.size());

        for (auto id : slice.regular_columns) {
            auto i = cells->find(id);
            if (i == cells->end()) {
                result_row.cells.emplace_back();
            } else {
                auto def = _schema->regular_column_at(id);
                if (def.is_atomic()) {
                    auto c = i->second.as_atomic_cell();
                    if (c.timestamp() < row_tombstone.timestamp) {
                        result_row.cells.emplace_back(std::experimental::make_optional(
                            atomic_cell_or_collection::from_atomic_cell(
                                atomic_cell::one::make_dead(row_tombstone.timestamp, row_tombstone.ttl))));
                    } else {
                        result_row.cells.emplace_back(std::experimental::make_optional(i->second));
                    }
                } else {
                    fail(unimplemented::cause::COLLECTIONS);
                }
            }
        }
        result.rows.emplace_back(row->key(), std::move(result_row));
        --limit;
    }

    if (!slice.static_columns.empty()) {
        // When there are no clustered rows, static row counts as one row with respect to row limit
        if (!result.rows.empty() || limit > 0)  {
            // FIXME: implement
            throw std::runtime_error("quering static columns not implemented");
        }
    }

    return result;
}

future<lw_shared_ptr<query::result>>
column_family::query(const query::read_command& cmd) {
    auto result = make_lw_shared<query::result>();

    uint32_t limit = cmd.row_limit;
    for (auto&& range : cmd.partition_ranges) {
        if (range.is_singular()) {
            auto& key = range.start_value();
            auto partition = find_partition(key);
            if (!partition) {
                return make_ready_future<lw_shared_ptr<query::result>>(result);
            }
            result->partitions.emplace_back(key,
                get_partition_slice(*partition, cmd.slice, limit));
            limit -= result->partitions.back().second.row_count();
            if (limit == 0) {
                return make_ready_future<lw_shared_ptr<query::result>>(result);
            }
        } else if (range.is_full()) {
            for (auto&& e : partitions) {
                auto& key = e.first;
                auto& partition = e.second;
                result->partitions.emplace_back(key,
                    get_partition_slice(partition, cmd.slice, limit));
                limit -= result->partitions.back().second.row_count();
                if (limit == 0) {
                    return make_ready_future<lw_shared_ptr<query::result>>(result);
                }
            }
        } else {
            fail(unimplemented::cause::RANGE_QUERIES);
        }
    }
    return make_ready_future<lw_shared_ptr<query::result>>(result);
}

future<lw_shared_ptr<query::result>>
database::query(const query::read_command& cmd) {
    static auto make_empty = [] {
        return make_ready_future<lw_shared_ptr<query::result>>(make_lw_shared(query::result()));
    };

    auto ks = find_keyspace(cmd.keyspace);
    if (!ks) {
        // FIXME: load from sstables
        return make_empty();
    }

    auto cf = ks->find_column_family(cmd.column_family);
    if (!cf) {
        return make_empty();
    }

    return cf->query(cmd);
}
