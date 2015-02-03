/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 */

#include "log.hh"
#include "database.hh"
#include "core/future-util.hh"

thread_local logging::logger dblog("database");

partition::partition(column_family& cf)
        : rows(key_compare(cf._schema->clustering_key_type)) {
}

template<typename Sequence>
std::vector<::shared_ptr<abstract_type>>
get_column_types(const Sequence& column_definitions) {
    std::vector<shared_ptr<abstract_type>> result;
    for (auto&& col : column_definitions) {
        result.push_back(col.type);
    }
    return result;
}

void
schema::build_columns(std::vector<column> columns, column_definition::column_kind kind,
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
    data_type regular_column_name_type)
        : _regular_columns_by_name(serialized_compare(regular_column_name_type))
        , ks_name(std::move(ks_name))
        , cf_name(std::move(cf_name))
        , partition_key_type(::make_shared<tuple_type<>>(get_column_types(partition_key)))
        , clustering_key_type(::make_shared<tuple_type<>>(get_column_types(clustering_key)))
        , clustering_key_prefix_type(::make_shared<tuple_prefix>(get_column_types(clustering_key)))
        , regular_column_name_type(regular_column_name_type)
{
    if (partition_key.size() == 1) {
        thrift.partition_key_type = partition_key[0].type;
    } else {
        // TODO: the type should be composite_type
        throw std::runtime_error("not implemented");
    }

    build_columns(std::move(partition_key), column_definition::PARTITION, this->partition_key);
    build_columns(std::move(clustering_key), column_definition::CLUSTERING, this->clustering_key);

    std::sort(regular_columns.begin(), regular_columns.end(), column::name_compare(regular_column_name_type));
    build_columns(std::move(regular_columns), column_definition::REGULAR, this->regular_columns);
    for (column_definition& def : this->regular_columns) {
        _regular_columns_by_name[def.name()] = &def;
    }
}

column_family::column_family(schema_ptr schema)
    : _schema(std::move(schema))
    , partitions(key_compare(_schema->thrift.partition_key_type)) {
}

partition*
column_family::find_partition(const bytes& key) {
    auto i = partitions.find(key);
    return i == partitions.end() ? nullptr : &i->second;
}

row*
column_family::find_row(const bytes& partition_key, const bytes& clustering_key) {
    partition* p = find_partition(partition_key);
    if (!p) {
        return nullptr;
    }
    auto i = p->rows.find(clustering_key);
    return i == p->rows.end() ? nullptr : &i->second;
}

partition&
column_family::find_or_create_partition(const bytes& key) {
    // call lower_bound so we have a hint for the insert, just in case.
    auto i = partitions.lower_bound(key);
    if (i == partitions.end() || key != i->first) {
        i = partitions.emplace_hint(i, std::make_pair(std::move(key), partition(*this)));
    }
    return i->second;
}

row&
column_family::find_or_create_row(const bytes& partition_key, const bytes& clustering_key) {
    partition& p = find_or_create_partition(partition_key);
    // call lower_bound so we have a hint for the insert, just in case.
    auto i = p.rows.lower_bound(clustering_key);
    if (i == p.rows.end() || clustering_key != i->first) {
        i = p.rows.emplace_hint(i, std::make_pair(std::move(clustering_key), row()));
    }
    return i->second;
}

sstring to_hex(const bytes& b) {
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

future<keyspace> keyspace::populate(sstring ksdir) {

    auto ks = make_lw_shared<keyspace>();
    return lister::scan_dir(ksdir, directory_entry_type::directory, [ks, ksdir] (directory_entry de) {
        auto comps = parse_fname(de.name);
        if (comps.size() != 2) {
            dblog.error("Keyspace {}: Skipping malformed CF {} ", ksdir, de.name);
            return make_ready_future<>();
        }
        sstring cfname = comps[0];

        auto sstdir = ksdir + "/" + de.name;
        dblog.warn("Keyspace {}: Reading CF {} ", ksdir, comps[0]);
        return make_ready_future<>();
    }).then([ks] {
        return make_ready_future<keyspace>(std::move(*ks));
    });
}

future<database> database::populate(sstring datadir) {

    auto db = make_lw_shared<database>();
    return lister::scan_dir(datadir, directory_entry_type::directory, [db, datadir] (directory_entry de) {
        dblog.warn("Populating Keyspace {}", de.name);
        auto ksdir = datadir + "/" + de.name;
        return keyspace::populate(ksdir).then([db, de] (keyspace ks){
            db->keyspaces[de.name] = std::move(ks);
        });
    }).then([db] {
        return make_ready_future<database>(std::move(*db));
    });
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

schema_ptr
keyspace::find_schema(sstring cf_name) {
    auto i = column_families.find(cf_name);
    if (i == column_families.end()) {
        return {};
    }
    return i->second._schema;
}

keyspace*
database::find_keyspace(sstring name) {
    auto i = keyspaces.find(name);
    if (i != keyspaces.end()) {
        return &i->second;
    }
    return nullptr;
}
