/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 */

#include "log.hh"
#include "database.hh"
#include "unimplemented.hh"
#include "core/future-util.hh"
#include "db/system_keyspace.hh"
#include "db/consistency_level.hh"
#include "db/serializer.hh"
#include "db/commitlog/commitlog.hh"
#include "to_string.hh"
#include "query-result-writer.hh"

#include "cql3/column_identifier.hh"
#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/split.hpp>
#include "sstables/sstables.hh"
#include <boost/range/adaptor/transformed.hpp>
#include "locator/simple_snitch.hh"

thread_local logging::logger dblog("database");

column_family::column_family(schema_ptr schema)
    : _schema(std::move(schema))
{ }

// define in .cc, since sstable is forward-declared in .hh
column_family::~column_family() {
}

mutation_partition*
column_family::find_partition(const dht::decorated_key& key) {
    auto i = partitions.find(key);
    return i == partitions.end() ? nullptr : &i->second;
}

mutation_partition*
column_family::find_partition_slow(const partition_key& key) {
    return find_partition(dht::global_partitioner().decorate_key(key));
}

row*
column_family::find_row(const dht::decorated_key& partition_key, const clustering_key& clustering_key) {
    mutation_partition* p = find_partition(partition_key);
    if (!p) {
        return nullptr;
    }
    return p->find_row(clustering_key);
}

mutation_partition&
column_family::find_or_create_partition_slow(const partition_key& key) {
    return find_or_create_partition(dht::global_partitioner().decorate_key(key));
}

mutation_partition&
column_family::find_or_create_partition(const dht::decorated_key& key) {
    // call lower_bound so we have a hint for the insert, just in case.
    auto i = partitions.lower_bound(key);
    if (i == partitions.end() || key != i->first) {
        i = partitions.emplace_hint(i, std::make_pair(std::move(key), mutation_partition(_schema)));
    }
    return i->second;
}

row&
column_family::find_or_create_row_slow(const partition_key& partition_key, const clustering_key& clustering_key) {
    mutation_partition& p = find_or_create_partition_slow(partition_key);
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
    return !b ? "null" : to_hex(*b);
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


database::database() {
    db::system_keyspace::make(*this);
}

database::~database() {
}

future<> database::populate(sstring datadir) {
    return lister::scan_dir(datadir, directory_entry_type::directory, [this, datadir] (directory_entry de) {
        auto& ks_name = de.name;
        auto ksdir = datadir + "/" + de.name;

        auto i = _keyspaces.find(ks_name);
        if (i == _keyspaces.end()) {
            dblog.warn("Skipping undefined keyspace: {}", ks_name);
        } else {
            dblog.warn("Populating Keyspace {}", ks_name);
            return lister::scan_dir(ksdir, directory_entry_type::directory, [this, ksdir, ks_name] (directory_entry de) {
                auto comps = parse_fname(de.name);
                if (comps.size() != 2) {
                    dblog.error("Keyspace {}: Skipping malformed CF {} ", ksdir, de.name);
                    return make_ready_future<>();
                }
                sstring cfname = comps[0];

                auto sstdir = ksdir + "/" + de.name;

                try {
                    auto& cf = find_column_family(ks_name, cfname);
                    dblog.info("Keyspace {}: Reading CF {} ", ksdir, cfname);
                    // FIXME: Increase parallelism.
                    return cf.populate(sstdir);
                } catch (no_such_column_family&) {
                    dblog.warn("{}, CF {}: schema not loaded!", ksdir, comps[0]);
                    return make_ready_future<>();
                }
            });
        }
        return make_ready_future<>();
    });
}

future<>
database::init_from_data_directory(sstring datadir) {
    return populate(datadir).then([this, datadir]() {
        return init_commitlog(datadir);
    });
}

future<>
database::init_commitlog(sstring datadir) {
    auto logdir = datadir + "/work" + std::to_string(engine().cpu_id());

    return engine().file_type(logdir).then([this, logdir](auto type) {
        if (type && type.value() != directory_entry_type::directory) {
            throw std::runtime_error("Not a directory " + logdir);
        }
        if (!type && ::mkdir(logdir.c_str(), S_IRWXU) != 0) {
            throw std::runtime_error("Could not create directory " + logdir);
        }

        db::commitlog::config cfg;
        cfg.commit_log_location = logdir;
        // TODO: real config. Real logging.
        // Right now we just set this up to use a single segment
        // and discard everything left on disk (not filling it)
        // with no hope of actually retrieving stuff...
        cfg.commitlog_total_space_in_mb = 1;

        return db::commitlog::create_commitlog(cfg).then([this](db::commitlog&& log) {
            _commitlog = std::make_unique<db::commitlog>(std::move(log));
        });
    });
}

unsigned
database::shard_of(const dht::token& t) {
    if (t._data.empty()) {
        return 0;
    }
    return uint8_t(t._data[0]) % smp::count;
}

unsigned
database::shard_of(const mutation& m) {
    return shard_of(m.token());
}

keyspace& database::add_keyspace(sstring name, keyspace k) {
    if (_keyspaces.count(name) != 0) {
        throw std::invalid_argument("Keyspace " + name + " already exists");
    }
    return _keyspaces.emplace(std::move(name), std::move(k)).first->second;
}

void database::update_keyspace(const sstring& name) {
    throw std::runtime_error("not implemented");
}

void database::drop_keyspace(const sstring& name) {
    throw std::runtime_error("not implemented");
}

void database::add_column_family(const utils::UUID& uuid, column_family&& cf) {
    if (_keyspaces.count(cf._schema->ks_name()) == 0) {
        throw std::invalid_argument("Keyspace " + cf._schema->ks_name() + " not defined");
    }
    if (_column_families.count(uuid) != 0) {
        throw std::invalid_argument("UUID " + uuid.to_sstring() + " already mapped");
    }
    auto kscf = std::make_pair(cf._schema->ks_name(), cf._schema->cf_name());
    if (_ks_cf_to_uuid.count(kscf) != 0) {
        throw std::invalid_argument("Column family " + cf._schema->cf_name() + " exists");
    }
    _column_families.emplace(uuid, std::move(cf));
    _ks_cf_to_uuid.emplace(std::move(kscf), uuid);
}

void database::add_column_family(column_family&& cf) {
    auto id = cf._schema->id();
    add_column_family(id, std::move(cf));
}

const utils::UUID& database::find_uuid(const sstring& ks, const sstring& cf) const throw (std::out_of_range) {
    return _ks_cf_to_uuid.at(std::make_pair(ks, cf));
}

const utils::UUID& database::find_uuid(const schema_ptr& schema) const throw (std::out_of_range) {
    return find_uuid(schema->ks_name(), schema->cf_name());
}

keyspace& database::find_keyspace(const sstring& name) throw (no_such_keyspace) {
    try {
        return _keyspaces.at(name);
    } catch (...) {
        std::throw_with_nested(no_such_keyspace(name));
    }
}

const keyspace& database::find_keyspace(const sstring& name) const throw (no_such_keyspace) {
    try {
        return _keyspaces.at(name);
    } catch (...) {
        std::throw_with_nested(no_such_keyspace(name));
    }
}

bool database::has_keyspace(const sstring& name) const {
    return _keyspaces.count(name) != 0;
}

column_family& database::find_column_family(const sstring& ks_name, const sstring& cf_name) throw (no_such_column_family) {
    try {
        return find_column_family(find_uuid(ks_name, cf_name));
    } catch (...) {
        std::throw_with_nested(no_such_column_family(ks_name + ":" + cf_name));
    }
}

const column_family& database::find_column_family(const sstring& ks_name, const sstring& cf_name) const throw (no_such_column_family) {
    try {
        return find_column_family(find_uuid(ks_name, cf_name));
    } catch (...) {
        std::throw_with_nested(no_such_column_family(ks_name + ":" + cf_name));
    }
}

column_family& database::find_column_family(const utils::UUID& uuid) throw (no_such_column_family) {
    try {
        return _column_families.at(uuid);
    } catch (...) {
        std::throw_with_nested(no_such_column_family(uuid.to_sstring()));
    }
}

const column_family& database::find_column_family(const utils::UUID& uuid) const throw (no_such_column_family) {
    try {
        return _column_families.at(uuid);
    } catch (...) {
        std::throw_with_nested(no_such_column_family(uuid.to_sstring()));
    }
}

void
keyspace::create_replication_strategy(config::ks_meta_data& ksm) {
    static thread_local locator::token_metadata tm;
    static locator::simple_snitch snitch;
    static std::unordered_map<sstring, sstring> options = {{"replication_factor", "3"}};
    auto d2t = [](double d) {
        unsigned long l = net::hton(static_cast<unsigned long>(d*(std::numeric_limits<unsigned long>::max())));
        std::array<int8_t, 8> a;
        memcpy(a.data(), &l, 8);
        return a;
    };
    tm.update_normal_token({dht::token::kind::key, {d2t(0).data(), 8}}, to_sstring("127.0.0.1"));
    tm.update_normal_token({dht::token::kind::key, {d2t(1.0/4).data(), 8}}, to_sstring("127.0.0.2"));
    tm.update_normal_token({dht::token::kind::key, {d2t(2.0/4).data(), 8}}, to_sstring("127.0.0.3"));
    tm.update_normal_token({dht::token::kind::key, {d2t(3.0/4).data(), 8}}, to_sstring("127.0.0.4"));
    _replication_strategy = locator::abstract_replication_strategy::create_replication_strategy(ksm.name, ksm.strategy_name, tm, snitch, options);
}

locator::abstract_replication_strategy&
keyspace::get_replication_strategy() {
    return *_replication_strategy;
}

column_family& database::find_column_family(const schema_ptr& schema) throw (no_such_column_family) {
    return find_column_family(schema->id());
}

const column_family& database::find_column_family(const schema_ptr& schema) const throw (no_such_column_family) {
    return find_column_family(schema->id());
}

schema_ptr database::find_schema(const sstring& ks_name, const sstring& cf_name) const throw (no_such_column_family) {
    return find_schema(find_uuid(ks_name, cf_name));
}

schema_ptr database::find_schema(const utils::UUID& uuid) const throw (no_such_column_family) {
    return find_column_family(uuid)._schema;
}

keyspace&
database::find_or_create_keyspace(const sstring& name) {
    auto i = _keyspaces.find(name);
    if (i != _keyspaces.end()) {
        return i->second;
    }
    return _keyspaces.emplace(name, keyspace()).first->second;
}

void
column_family::apply(const mutation& m) {
    mutation_partition& p = find_or_create_partition(m.decorated_key());
    p.apply(_schema, m.partition());
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

future<lw_shared_ptr<query::result>>
column_family::query(const query::read_command& cmd) {
    query::result::builder builder(cmd.slice);

    uint32_t limit = cmd.row_limit;
    for (auto&& range : cmd.partition_ranges) {
        if (limit == 0) {
            break;
        }
        if (range.is_singular()) {
            auto& key = range.start_value();
            auto partition = find_partition_slow(key);
            if (!partition) {
                break;
            }
            auto p_builder = builder.add_partition(key);
            partition->query(*_schema, cmd.slice, limit, p_builder);
            p_builder.finish();
            limit -= p_builder.row_count();
        } else if (range.is_full()) {
            for (auto&& e : partitions) {
                auto& dk = e.first;
                auto& partition = e.second;
                auto p_builder = builder.add_partition(dk._key);
                partition.query(*_schema, cmd.slice, limit, p_builder);
                p_builder.finish();
                limit -= p_builder.row_count();
                if (limit == 0) {
                    break;
                }
            }
        } else {
            fail(unimplemented::cause::RANGE_QUERIES);
        }
    }
    return make_ready_future<lw_shared_ptr<query::result>>(
            make_lw_shared<query::result>(builder.build()));
}

future<lw_shared_ptr<query::result>>
database::query(const query::read_command& cmd) {
    static auto make_empty = [] {
        return make_ready_future<lw_shared_ptr<query::result>>(make_lw_shared(query::result()));
    };

    try {
        column_family& cf = find_column_family(cmd.cf_id);
        return cf.query(cmd);
    } catch (...) {
        // FIXME: load from sstables
        return make_empty();
    }
}

std::ostream& operator<<(std::ostream& out, const atomic_cell_or_collection& c) {
    return out << to_hex(c._data);
}

void print_partition(std::ostream& out, const schema& s, const mutation_partition& mp) {
    out << "{rows={\n";
    for (auto&& e : mp.range(s, query::range<clustering_key_prefix>())) {
        out << e.key() << " => ";
        for (auto&& cell_e : e.row().cells) {
            out << cell_e.first << ":";
            out << cell_e.second << " ";
        }
        out << "\n";
    }
    out << "}}";
}

std::ostream& operator<<(std::ostream& os, const mutation& m) {
    fprint(os, "{mutation: schema %p key %s data ", m.schema().get(), m.key());
    print_partition(os, *m.schema(), m.partition());
    os << "}";
    return os;
}

std::ostream& operator<<(std::ostream& out, const column_family& cf) {
    out << "{\n";
    for (auto&& e : cf.partitions) {
        out << e.first << " => ";
        print_partition(out, *cf._schema, e.second);
        out << "\n";
    }
    out << "}";
    return out;
}

std::ostream& operator<<(std::ostream& out, const database& db) {
    out << "{\n";
    for (auto&& e : db._column_families) {
        auto&& cf = e.second;
        out << "(" << e.first.to_sstring() << ", " << cf._schema->cf_name() << ", " << cf._schema->ks_name() << "): " << cf << "\n";
    }
    out << "}";
    return out;
}

future<> database::apply_in_memory(const mutation& m) {
    try {
        auto& cf = find_column_family(m.schema());
        cf.apply(m);
    } catch (no_such_column_family&) {
        // TODO: log a warning
        // FIXME: load keyspace meta-data from storage
    }
    return make_ready_future<>();
}

future<> database::apply(const mutation& m) {
    // I'm doing a nullcheck here since the init code path for db etc
    // is a little in flux and commitlog is created only when db is
    // initied from datadir.
    if (_commitlog != nullptr) {
        db::serializer<mutation> ms(*this, m);
        auto uuid = m.schema()->id();
        return _commitlog->add_mutation(uuid, ms.size(), ms).then([&m, this](auto rp) {
            return this->apply_in_memory(m);
        });
    }
    return apply_in_memory(m);
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
operator<<(std::ostream& os, const exploded_clustering_prefix& ecp) {
    // Can't pass to_hex() to transformed(), since it is overloaded, so wrap:
    auto enhex = [] (auto&& x) { return to_hex(x); };
    return fprint(os, "prefix{%s}", ::join(":", ecp._v | boost::adaptors::transformed(enhex)));
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

future<>
database::stop() {
    return make_ready_future<>();
}

