/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 */

#include "log.hh"
#include "database.hh"
#include "unimplemented.hh"
#include "core/future-util.hh"
#include "db/system_keyspace.hh"
#include "db/consistency_level.hh"

#include "cql3/column_identifier.hh"
#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/split.hpp>
#include "sstables/sstables.hh"

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
schema::make_column_specification(const column_definition& def) {
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
        def.column_specification = make_column_specification(def);
    }
}

void schema::rehash_columns() {
    _columns_by_name.clear();
    _regular_columns_by_name.clear();

    for (const column_definition& def : all_columns_in_select_order()) {
        _columns_by_name[def.name()] = &def;
    }

    for (const column_definition& def : _regular_columns) {
        _regular_columns_by_name[def.name()] = &def;
    }
}

schema::schema(sstring ks_name, sstring cf_name, std::vector<column> partition_key,
    std::vector<column> clustering_key,
    std::vector<column> regular_columns,
    std::vector<column> static_columns,
    data_type regular_column_name_type,
    sstring comment)
        : _regular_columns_by_name(serialized_compare(regular_column_name_type))
{
    this->_comment = std::move(comment);
    this->ks_name = std::move(ks_name);
    this->cf_name = std::move(cf_name);
    this->partition_key_type = ::make_lw_shared<tuple_type<>>(get_column_types(partition_key));
    this->clustering_key_type = ::make_lw_shared<tuple_type<>>(get_column_types(clustering_key));
    this->clustering_key_prefix_type = ::make_lw_shared(clustering_key_type->as_prefix());
    this->regular_column_name_type = regular_column_name_type;

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

    std::sort(static_columns.begin(), static_columns.end(), column::name_compare(utf8_type));
    build_columns(static_columns, column_definition::column_kind::STATIC, _static_columns);

    rehash_columns();
}

schema::schema(const schema& o)
        : raw_schema(o)
        , _regular_columns_by_name(serialized_compare(regular_column_name_type)) {
    rehash_columns();
}

column_family::column_family(schema_ptr schema)
    : _schema(std::move(schema))
    , partitions(partition_key::less_compare(*_schema)) {
}

mutation_partition*
column_family::find_partition(const partition_key& key) {
    auto i = partitions.find(key);
    return i == partitions.end() ? nullptr : &i->second;
}

row*
column_family::find_row(const partition_key& partition_key, const clustering_key& clustering_key) {
    mutation_partition* p = find_partition(partition_key);
    if (!p) {
        return nullptr;
    }
    return p->find_row(clustering_key);
}

mutation_partition&
column_family::find_or_create_partition(const partition_key& key) {
    // call lower_bound so we have a hint for the insert, just in case.
    auto i = partitions.lower_bound(key);
    if (i == partitions.end() || !key.equal(*_schema, i->first)) {
        i = partitions.emplace_hint(i, std::make_pair(std::move(key), mutation_partition(_schema)));
    }
    return i->second;
}

row&
column_family::find_or_create_row(const partition_key& partition_key, const clustering_key& clustering_key) {
    mutation_partition& p = find_or_create_partition(partition_key);
    return p.clustered_row(clustering_key).cells;
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

future<> column_family::probe_file(sstring sstdir, sstring fname) {

    using namespace sstables;

    auto comps = parse_fname(fname);
    if (comps.size() != 5) {
        dblog.error("Ignoring malformed file {}", fname);
        return make_ready_future<>();
    }

    // Every table will have a TOC. Using a specific file as a criteria, as
    // opposed to, say verifying _sstables.count() to be zero is more robust
    // against parallel loading of the directory contents.
    if (comps[3] != "TOC") {
        return make_ready_future<>();
    }

    sstable::version_types version;
    sstable::format_types  format;

    try {
        version = sstable::version_from_sstring(comps[0]);
    } catch (std::out_of_range) {
        dblog.error("Uknown version found: {}", comps[0]);
        return make_ready_future<>();
    }

    auto generation = boost::lexical_cast<unsigned long>(comps[1]);

    try {
        format = sstable::format_from_sstring(comps[2]);
    } catch (std::out_of_range) {
        dblog.error("Uknown format found: {}", comps[2]);
        return make_ready_future<>();
    }

    assert(_sstables.count(generation) == 0);

    try {
        auto sst = std::make_unique<sstables::sstable>(sstdir, generation, version, format);
        auto fut = sst->load();
        return std::move(fut).then([this, generation, sst = std::move(sst)] () mutable {
            _sstables.emplace(generation, std::move(sst));
            return make_ready_future<>();
        });
    } catch (malformed_sstable_exception& e) {
        dblog.error("Skipping malformed sstable: {}", e.what());
        return make_ready_future<>();
    }

    return make_ready_future<>();
}

future<> column_family::populate(sstring sstdir) {

    return lister::scan_dir(sstdir, directory_entry_type::regular, [this, sstdir] (directory_entry de) {
        // FIXME: The secondary indexes are in this level, but with a directory type, (starting with ".")
        return probe_file(sstdir, de.name);
    });
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
        if (column_families.count(cfname) != 0) {
            dblog.info("Keyspace {}: Reading CF {} ", ksdir, comps[0]);

            // FIXME: Increase parallelism.
            return column_families.at(cfname).populate(sstdir);
        } else {
            dblog.warn("{}, CF {}: schema not loaded!", ksdir, comps[0]);
            return make_ready_future<>();
        }
    });
}

database::database() {
    keyspaces.emplace("system", db::system_keyspace::make());
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

const column_definition* schema::get_column_definition(const bytes& name) {
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

keyspace&
database::find_or_create_keyspace(const sstring& name) {
    auto i = keyspaces.find(name);
    if (i != keyspaces.end()) {
        return i->second;
    }
    return keyspaces.emplace(name, keyspace()).first->second;
}

void
column_family::apply(const mutation& m) {
    mutation_partition& p = find_or_create_partition(m.key);
    p.apply(_schema, m.p);
}

// Based on org.apache.cassandra.db.AbstractCell#reconcile()
int
compare_atomic_cell_for_merge(atomic_cell_view left, atomic_cell_view right) {
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
            i->row().t.apply(entry.row().t);
            i->row().created_at = std::max(i->row().created_at, entry.row().created_at);
            merge_cells(i->row().cells, entry.row().cells, find_regular_column_def);
        }
    }
}

tombstone
mutation_partition::range_tombstone_for_row(const schema& schema, const clustering_key& key) {
    tombstone t = _tombstone;

    if (_row_tombstones.empty()) {
        return t;
    }

    auto c = row_tombstones_entry::key_comparator(
        clustering_key::prefix_view_type::less_compare_with_prefix(schema));

    // _row_tombstones contains only strict prefixes
    for (unsigned prefix_len = 1; prefix_len < schema.clustering_key_size(); ++prefix_len) {
        auto i = _row_tombstones.find(key.prefix_view(schema, prefix_len), c);
        if (i != _row_tombstones.end()) {
            t.apply(i->t());
        }
    }

    return t;
}

tombstone
mutation_partition::tombstone_for_row(const schema& schema, const clustering_key& key) {
    tombstone t = range_tombstone_for_row(schema, key);

    auto j = _rows.find(key, rows_entry::compare(schema));
    if (j != _rows.end()) {
        t.apply(j->row().t);
    }

    return t;
}

tombstone
mutation_partition::tombstone_for_row(const schema& schema, const rows_entry& e) {
    tombstone t = range_tombstone_for_row(schema, e.key());
    t.apply(e.row().t);
    return t;
}

void
mutation_partition::apply_row_tombstone(schema_ptr schema, clustering_key_prefix prefix, tombstone t) {
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
mutation_partition::apply_delete(schema_ptr schema, const exploded_clustering_prefix& prefix, tombstone t) {
    if (!prefix) {
        apply(t);
    } else if (prefix.is_full(*schema)) {
        apply_delete(schema, clustering_key::from_clustering_prefix(*schema, prefix), t);
    } else {
        apply_row_tombstone(schema, clustering_key_prefix::from_clustering_prefix(*schema, prefix), t);
    }
}

void
mutation_partition::apply_delete(schema_ptr schema, clustering_key&& key, tombstone t) {
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
mutation_partition::find_entry(schema_ptr schema, const clustering_key_prefix& key) {
    auto i = _rows.find(key, rows_entry::key_comparator(clustering_key::less_compare_with_prefix(*schema)));
    if (i == _rows.end()) {
        return nullptr;
    }
    return &*i;
}

row*
mutation_partition::find_row(const clustering_key& key) {
    auto i = _rows.find(key);
    if (i == _rows.end()) {
        return nullptr;
    }
    return &i->row().cells;
}

deletable_row&
mutation_partition::clustered_row(const clustering_key& key) {
    auto i = _rows.find(key);
    if (i == _rows.end()) {
        auto e = new rows_entry(key);
        _rows.insert(i, *e);
        return e->row();
    }
    return i->row();
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

boost::iterator_range<mutation_partition::rows_type::iterator>
mutation_partition::range(const schema& schema, const query::range<clustering_key_prefix>& r) {
    if (r.is_full()) {
        return boost::make_iterator_range(_rows.begin(), _rows.end());
    }
    auto cmp = rows_entry::key_comparator(clustering_key::prefix_equality_less_compare(schema));
    if (r.is_singular()) {
        auto&& prefix = r.start()->value();
        return boost::make_iterator_range(_rows.lower_bound(prefix, cmp), _rows.upper_bound(prefix, cmp));
    }
    auto i1 = r.start() ? (r.start()->is_inclusive()
            ? _rows.lower_bound(r.start()->value(), cmp)
            : _rows.upper_bound(r.start()->value(), cmp)) : _rows.begin();
    auto i2 = r.end() ? (r.end()->is_inclusive()
            ? _rows.upper_bound(r.end()->value(), cmp)
            : _rows.lower_bound(r.end()->value(), cmp)) : _rows.end();
    return boost::make_iterator_range(i1, i2);
}

void mutation::set_static_cell(const column_definition& def, atomic_cell_or_collection value) {
    update_column(p.static_row(), def, std::move(value));
}

void mutation::set_clustered_cell(const exploded_clustering_prefix& prefix, const column_definition& def, atomic_cell_or_collection value) {
    auto& row = p.clustered_row(clustering_key::from_clustering_prefix(*schema, prefix)).cells;
    update_column(row, def, std::move(value));
}

void mutation::set_clustered_cell(const clustering_key& key, const column_definition& def, atomic_cell_or_collection value) {
    auto& row = p.clustered_row(key).cells;
    update_column(row, def, std::move(value));
}

void mutation::set_cell(const exploded_clustering_prefix& prefix, const bytes& name, const boost::any& value,
        api::timestamp_type timestamp, ttl_opt ttl) {
    auto column_def = schema->get_column_definition(name);
    if (!column_def) {
        throw std::runtime_error(sprint("no column definition found for '%s'", name));
    }
    return set_cell(prefix, *column_def, atomic_cell::make_live(timestamp, ttl, column_def->type->decompose(value)));
}

void mutation::set_cell(const exploded_clustering_prefix& prefix, const column_definition& def, atomic_cell_or_collection value) {
    if (def.is_static()) {
        set_static_cell(def, std::move(value));
    } else if (def.is_regular()) {
        set_clustered_cell(prefix, def, std::move(value));
    } else {
        throw std::runtime_error("attemting to store into a key cell");
    }
}

std::experimental::optional<atomic_cell_or_collection>
mutation::get_cell(const clustering_key& rkey, const column_definition& def) {
    auto find_cell = [&def] (row& r) {
        auto i = r.find(def.id);
        if (i == r.end()) {
            return std::experimental::optional<atomic_cell_or_collection>{};
        }
        return std::experimental::optional<atomic_cell_or_collection>{i->second};
    };
    if (def.is_static()) {
        return find_cell(p.static_row());
    } else {
        auto r = p.find_row(rkey);
        if (!r) {
            return {};
        }
        return find_cell(*r);
    }
}

void mutation::update_column(row& row, const column_definition& def, atomic_cell_or_collection&& value) {
    // our mutations are not yet immutable
    auto id = def.id;
    auto i = row.lower_bound(id);
    if (i == row.end() || i->first != id) {
        row.emplace_hint(i, id, std::move(value));
    } else {
        merge_column(def, i->second, value);
    }
}

template <typename ColumnDefResolver>
static query::result::row get_row_slice(const row& cells, const std::vector<column_id>& columns, tombstone tomb,
        ColumnDefResolver&& id_to_def) {
    query::result::row result_row;
    result_row.cells.reserve(columns.size());
    for (auto id : columns) {
        auto i = cells.find(id);
        if (i == cells.end()) {
            result_row.cells.emplace_back();
        } else {
            auto def = id_to_def(id);
            if (def.is_atomic()) {
                auto c = i->second.as_atomic_cell();
                if (!c.is_live(tomb)) {
                    result_row.cells.emplace_back();
                } else {
                    result_row.cells.emplace_back(std::experimental::make_optional(i->second));
                }
            } else {
                auto&& cell = i->second.as_collection_mutation();
                auto&& ctype = static_pointer_cast<collection_type_impl>(def.type);
                auto m_view = ctype->deserialize_mutation_form(cell);
                m_view.tomb.apply(tomb);
                auto m_ser = ctype->serialize_mutation_form_only_live(m_view);
                if (ctype->is_empty(m_ser)) {
                    result_row.cells.emplace_back();
                } else {
                    result_row.cells.emplace_back(std::experimental::make_optional(
                        atomic_cell_or_collection::from_collection_mutation(std::move(m_ser))));
                }
            }
        }
    }
    return result_row;
}

template <typename ColumnDefResolver>
bool has_any_live_data(const row& cells, tombstone tomb, ColumnDefResolver&& id_to_def) {
    for (auto&& e : cells) {
        auto&& cell_or_collection = e.second;
        const column_definition& def = id_to_def(e.first);
        if (def.is_atomic()) {
            auto&& c = cell_or_collection.as_atomic_cell();
            if (c.is_live(tomb)) {
                return true;
            }
        } else {
            auto&& cell = cell_or_collection.as_collection_mutation();
            auto&& ctype = static_pointer_cast<collection_type_impl>(def.type);
            if (ctype->is_any_live(cell, tomb)) {
                return true;
            }
        }
    }
    return false;
}

query::result::partition
column_family::get_partition_slice(mutation_partition& partition, const query::partition_slice& slice, uint32_t limit) {
    query::result::partition result;

    auto regular_column_resolver = [this] (column_id id) {
        return _schema->regular_column_at(id);
    };

    for (auto&& range : slice.row_ranges) {
        if (limit == 0) {
            break;
        }

        // FIXME: Optimize for a full-tuple singular range. mutation_partition::range()
        // does two lookups to form a range, even for singular range. We need
        // only one lookup for a full-tuple singular range though.
        for (auto&& e : partition.range(*_schema, range)) {
            auto& row = e.row();
            auto&& cells = row.cells;

            auto row_tombstone = partition.tombstone_for_row(*_schema, e);
            auto result_row = get_row_slice(cells, slice.regular_columns, row_tombstone, regular_column_resolver);
            auto row_is_live = row.created_at > row_tombstone.timestamp;

            // row_is_live is true for rows created using 'insert' statement
            // which are not deleted yet. Such rows are considered as present
            // even if no regular columns are live. Otherwise, a row is
            // considered present if it has any cell which is live. So if
            // we've got no live cell in the results we still have to check if
            // any of the row's cell is live and we should return the row in
            // such case.
            if (row_is_live || !result_row.all_cells_empty() || has_any_live_data(cells, row_tombstone, regular_column_resolver)) {
                result.rows.emplace_back(e.key(), std::move(result_row));
                if (--limit == 0) {
                    break;
                }
            }
        }
    }

    if (!slice.static_columns.empty()) {
        // When there are no clustered rows, static row counts as one row with respect to row limit
        if (!result.rows.empty() || limit > 0) {
            result.static_row = get_row_slice(partition.static_row(), slice.static_columns, partition.tombstone_for_static_row(),
                [this] (column_id id) { return _schema->static_column_at(id); });
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

namespace db {

std::ostream& operator<<(std::ostream& os, db::consistency_level cl) {
    switch (cl) {
    case db::consistency_level::ANY: return os << "ANY";
    case db::consistency_level::ONE: return os << "ONE";
    case db::consistency_level::TWO: return os << "TWO";
    case db::consistency_level::THREE: return os << "THREE";
    case db::consistency_level::QUORUM: return os << "QUORUM";
    case db::consistency_level::ALL: return os << "ALL";
    case db::consistency_level::LOCAL_QUORUM: return os << "LOCAL_QUORUM";
    case db::consistency_level::EACH_QUORUM: return os << "EACH_QUORUM";
    case db::consistency_level::SERIAL: return os << "SERIAL";
    case db::consistency_level::LOCAL_SERIAL: return os << "LOCAL_SERIAL";
    case db::consistency_level::LOCAL_ONE: return os << "LOCAL";
    default: abort();
    }
}

}

std::ostream&
operator<<(std::ostream& os, const atomic_cell_view& acv) {
    return fprint(os, "atomic_cell{%s;ts=%d;ttl=%d}",
            (acv.is_live() ? to_hex(acv.value()) : sstring("DEAD")),
            acv.timestamp(),
            acv.is_live_and_has_ttl() ? acv.ttl()->time_since_epoch().count() : -1);
}

std::ostream&
operator<<(std::ostream& os, const atomic_cell& ac) {
    return os << atomic_cell_view(ac);
}
