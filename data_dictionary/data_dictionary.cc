/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include <ranges>
#include "data_dictionary.hh"
#include "cql3/description.hh"
#include "impl.hh"
#include "user_types_metadata.hh"
#include "keyspace_metadata.hh"
#include "schema/schema.hh"
#include "cql3/util.hh"
#include "gms/feature_service.hh"
#include <fmt/core.h>
#include <fmt/ranges.h>
#include <fmt/std.h>
#include <ios>
#include <ostream>
#include <array>
#include "replica/database.hh"

namespace data_dictionary {

schema_ptr
table::schema() const {
    return _ops->get_table_schema(*this);
}

const std::vector<view_ptr>&
table::views() const {
    return _ops->get_table_views(*this);
}

const secondary_index::secondary_index_manager&
table::get_index_manager() const {
    return _ops->get_index_manager(*this);
}

lw_shared_ptr<keyspace_metadata>
keyspace::metadata() const {
    return _ops->get_keyspace_metadata(*this);
}

const user_types_metadata&
keyspace::user_types() const {
    return metadata()->user_types();
}

bool
keyspace::is_internal() const {
    return _ops->is_internal(*this);
}

bool
keyspace::uses_tablets() const {
    return metadata()->uses_tablets();
}

const locator::abstract_replication_strategy&
keyspace::get_replication_strategy() const {
    return _ops->get_replication_strategy(*this);
}

const table_schema_version&
database::get_version() const {
    return _ops->get_version(*this);
}

std::optional<keyspace>
database::try_find_keyspace(std::string_view name) const {
    return _ops->try_find_keyspace(*this, name);
}

bool
database::has_keyspace(std::string_view name) const {
    return bool(try_find_keyspace(name));
}

keyspace
database::find_keyspace(std::string_view name) const {
    auto ks = try_find_keyspace(name);
    if (!ks) {
        throw no_such_keyspace(name);
    }
    return *ks;
}

std::vector<keyspace>
database::get_keyspaces() const {
    return _ops->get_keyspaces(*this);
}

std::vector<sstring> 
database::get_user_keyspaces() const {
    return _ops->get_user_keyspaces(*this);
}

std::vector<sstring> 
database::get_all_keyspaces() const {
    return _ops->get_all_keyspaces(*this);
}

std::vector<table>
database::get_tables() const {
    return _ops->get_tables(*this);
}

std::optional<table>
database::try_find_table(std::string_view ks, std::string_view table) const {
    return _ops->try_find_table(*this, ks, table);
}

table
database::find_table(std::string_view ks, std::string_view table) const {
    auto t = try_find_table(ks, table);
    if (!t) {
        throw no_such_column_family(ks, table);
    }
    return *t;
}

std::optional<table>
database::try_find_table(table_id id) const {
    return _ops->try_find_table(*this, id);
}

table
database::find_column_family(table_id uuid) const {
    auto t = try_find_table(uuid);
    if (!t) {
        throw no_such_column_family(uuid);
    }
    return *t;
}

schema_ptr
database::find_schema(std::string_view ks, std::string_view table) const {
    return find_table(ks, table).schema();
}

schema_ptr
database::find_schema(table_id uuid) const {
    return find_column_family(uuid).schema();
}

bool
database::has_schema(std::string_view ks_name, std::string_view cf_name) const {
    return bool(try_find_table(ks_name, cf_name));
}

table
database::find_column_family(schema_ptr s) const {
    return find_column_family(s->id());
}

schema_ptr
database::find_indexed_table(std::string_view ks_name, std::string_view index_name) const {
    return _ops->find_indexed_table(*this, ks_name, index_name);
}

sstring
database::get_available_index_name(std::string_view ks_name, std::string_view table_name,
        std::optional<sstring> index_name_root) const {
    return _ops->get_available_index_name(*this, ks_name, table_name, index_name_root);
}

std::set<sstring>
database::existing_index_names(std::string_view ks_name, std::string_view cf_to_exclude) const {
    return _ops->existing_index_names(*this, ks_name, cf_to_exclude);
}

schema_ptr
database::get_cdc_base_table(std::string_view ks_name, std::string_view table_name) const {
    return get_cdc_base_table(*find_table(ks_name, table_name).schema());
}

schema_ptr
database::get_cdc_base_table(const schema& s) const {
    return _ops->get_cdc_base_table(*this, s);
}

const db::extensions& 
database::extensions() const {
    return _ops->get_extensions(*this);
}

const gms::feature_service&
database::features() const {
    return _ops->get_features(*this);
}

const db::config&
database::get_config() const {
    return _ops->get_config(*this);
}

replica::database&
database::real_database() const {
    return _ops->real_database(*this);
}

replica::database* database::real_database_ptr() const {
    return _ops->real_database_ptr(*this);
}

impl::~impl() = default;

keyspace_metadata::keyspace_metadata(std::string_view name,
             std::string_view strategy_name,
             locator::replication_strategy_config_options strategy_options,
             std::optional<unsigned> initial_tablets,
             bool durable_writes,
             std::vector<schema_ptr> cf_defs,
             user_types_metadata user_types,
             storage_options storage_opts)
    : _name{name}
    , _strategy_name{locator::abstract_replication_strategy::to_qualified_class_name(strategy_name.empty() ? "NetworkTopologyStrategy" : strategy_name)}
    , _strategy_options{std::move(strategy_options)}
    , _initial_tablets(initial_tablets)
    , _durable_writes{durable_writes}
    , _user_types{std::move(user_types)}
    , _storage_options(make_lw_shared<storage_options>(std::move(storage_opts)))
{
    for (auto&& s : cf_defs) {
        _cf_meta_data.emplace(s->cf_name(), s);
    }
}

void keyspace_metadata::validate(const gms::feature_service& fs, const locator::topology& topology) const {
    using namespace locator;
    locator::replication_strategy_params params(strategy_options(), initial_tablets());
    auto strategy = locator::abstract_replication_strategy::create_replication_strategy(strategy_name(), params);
    strategy->validate_options(fs, topology);
}

lw_shared_ptr<keyspace_metadata>
keyspace_metadata::new_keyspace(std::string_view name,
                                std::string_view strategy_name,
                                locator::replication_strategy_config_options options,
                                std::optional<unsigned> initial_tablets,
                                bool durables_writes,
                                storage_options storage_opts,
                                std::vector<schema_ptr> cf_defs)
{
    return ::make_lw_shared<keyspace_metadata>(name, strategy_name, options, initial_tablets, durables_writes, cf_defs, user_types_metadata{}, storage_opts);
}

lw_shared_ptr<keyspace_metadata>
keyspace_metadata::new_keyspace(const keyspace_metadata& ksm) {
    return new_keyspace(ksm.name(), ksm.strategy_name(), ksm.strategy_options(), ksm.initial_tablets(), ksm.durable_writes(), ksm.get_storage_options());
}

void keyspace_metadata::add_user_type(const user_type ut) {
    _user_types.add_type(ut);
}

void keyspace_metadata::remove_user_type(const user_type ut) {
    _user_types.remove_type(ut);
}

std::vector<schema_ptr> keyspace_metadata::tables() const {
    return _cf_meta_data
            | std::views::values
            | std::views::filter([] (const auto& s) { return !s->is_view(); })
            | std::ranges::to<std::vector<schema_ptr>>();
}

std::vector<view_ptr> keyspace_metadata::views() const {
    return _cf_meta_data
            | std::views::values
            | std::views::filter([] (const auto& s) { return s->is_view(); })
            | std::views::transform([] (auto& s) { return view_ptr(s); })
            | std::ranges::to<std::vector<view_ptr>>();
}

storage_options::local storage_options::local::from_map(const std::map<sstring, sstring>& values) {
    if (!values.empty()) {
        throw std::runtime_error("Local storage does not accept any custom options");
    }
    return {};
}

std::map<sstring, sstring> storage_options::local::to_map() const {
    return {};
}

storage_options::s3 storage_options::s3::from_map(const std::map<sstring, sstring>& values) {
    s3 options;
    const std::array<std::pair<sstring, sstring*>, 2> allowed_options {
        std::make_pair("bucket", &options.bucket),
        std::make_pair("endpoint", &options.endpoint),
    };
    for (auto& option : allowed_options) {
        if (auto it = values.find(option.first); it != values.end()) {
            *option.second = it->second;
        } else {
            throw std::runtime_error(fmt::format("Missing S3 option: {}", option.first));
        }
    }
    if (values.size() > allowed_options.size()) {
        throw std::runtime_error(fmt::format("Extraneous options for S3: {}; allowed: {}",
            fmt::join(values | std::views::keys, ","),
            fmt::join(allowed_options | std::views::keys, ",")));
    }
    return options;
}

std::map<sstring, sstring> storage_options::s3::to_map() const {
    return {{"bucket", bucket},
            {"endpoint", endpoint}};
}

bool storage_options::is_local_type() const noexcept {
    return std::holds_alternative<local>(value);
}

storage_options::value_type storage_options::from_map(std::string_view type, std::map<sstring, sstring> values) {
    if (type == local::name) {
        return local::from_map(values);
    }
    if (type == s3::name) {
        return s3::from_map(values);
    }
    throw std::runtime_error(fmt::format("Unknown storage type: {}", type));
}

std::string_view storage_options::type_string() const {
    return std::visit([] (auto& opt) { return opt.name; }, value);
}

std::map<sstring, sstring> storage_options::to_map() const {
    return std::visit([] (auto& opt) { return opt.to_map(); }, value);
}

bool storage_options::can_update_to(const storage_options& new_options) {
    return value == new_options.value;
}

storage_options storage_options::append_to_s3_prefix(const sstring& s) const {
    // when restoring from object storage, the API of /storage_service/restore
    // provides:
    // 1. a shared prefix
    // 2. a list of sstables, each of which has its own partial path
    //
    // for example, assuming we have following API call:
    // - shared prefix: /bucket/ks/cf
    // - sstables
    //   - 3123/me-3gdq_0bki_2cvk01yl83nj0tp5gh-big-TOC.txt
    //   - 3123/me-3gdq_0bki_2edkg2vx4xtksugjj5-big-TOC.txt
    //   - 3245/me-3gdq_0bki_2cvk02wubgncy8qd41-big-TOC.txt
    //
    // note, this example shows three sstables from two different snapshot backups.
    //
    // we assume all sstables' locations share the same base prefix (storage_options::s3::prefix).
    // however, sstable in different backups have different prefixes. to handle this, we compose
    // a per-sstable prefix by concatenating the shared prefix and the "parent directory" of the
    // sstable's location. the resulting structure looks like:
    //
    // sstables:
    //   - prefix: /bucket/ks/cf/3123
    //     desc: me-3gdq_0bki_2cvk01yl83nj0tp5gh-big
    //   - prefix: /bucket/ks/cf/3123
    //     desc: me-3gdq_0bki_2edkg2vx4xtksugjj5-big
    //   - prefix: /bucket/ks/cf/3145
    //     desc: me-3gdq_0bki_2cvk02wubgncy8qd41-big
    SCYLLA_ASSERT(!is_local_type());
    storage_options ret = *this;
    if (s.empty()) {
        // scylla-manager should always pass sstables with non-empty dirname,
        // but still..
        return ret;
    }

    s3 s3_options = std::get<s3>(value);
    SCYLLA_ASSERT(std::holds_alternative<sstring>(s3_options.location));
    sstring prefix = std::get<sstring>(s3_options.location);
    s3_options.location = seastar::format("{}/{}", prefix, s);
    ret.value = std::move(s3_options);
    return ret;
}

no_such_keyspace::no_such_keyspace(std::string_view ks_name)
    : runtime_error{fmt::format("Can't find a keyspace {}", ks_name)}
{
}

no_such_column_family::no_such_column_family(const table_id& uuid)
    : runtime_error{fmt::format("Can't find a column family with UUID {}", uuid)}
{
}

no_such_column_family::no_such_column_family(std::string_view ks_name, std::string_view cf_name)
    : runtime_error{fmt::format("Can't find a column family {} in keyspace {}", cf_name, ks_name)}
{
}

no_such_column_family::no_such_column_family(std::string_view ks_name, const table_id& uuid)
    : runtime_error{fmt::format("Can't find a column family with UUID {} in keyspace {}", uuid, ks_name)}
{
}

cql3::description keyspace_metadata::describe(const replica::database& db, cql3::with_create_statement with_create_statement) const {
    auto maybe_create_statement = std::invoke([&] -> std::optional<managed_string> {
        if (!with_create_statement) {
            return std::nullopt;
        }

        fragmented_ostringstream os;

        os << "CREATE KEYSPACE " << cql3::util::maybe_quote(_name)
           << " WITH replication = {'class': " << cql3::util::single_quote(_strategy_name);
        for (const auto& opt: _strategy_options) {
            os << ", " << cql3::util::single_quote(opt.first) << ": " << cql3::util::single_quote(opt.second);
        }
        if (!_storage_options->is_local_type()) {
            os << "} AND storage = {'type': " << cql3::util::single_quote(sstring(_storage_options->type_string()));
            for (const auto& e : _storage_options->to_map()) {
                os << ", " << cql3::util::single_quote(e.first) << ": " << cql3::util::single_quote(e.second);
            }
        }
        os << "} AND durable_writes = " << fmt::to_string(_durable_writes);
        if (db.features().tablets) {
            if (!_initial_tablets.has_value()) {
                os << " AND tablets = {'enabled': false}";
            } else {
                os << format(" AND tablets = {{'enabled': true{}}}", _initial_tablets.value() ? format(", 'initial': {}", _initial_tablets.value()) : "");
            }
        }
        os << ";";

        return std::move(os).to_managed_string();
    });

    return cql3::description {
        .keyspace = name(),
        .type = "keyspace",
        .name = name(),
        .create_statement = std::move(maybe_create_statement)
    };
}

} // namespace data_dictionary

template <>
struct fmt::formatter<data_dictionary::user_types_metadata> {
    constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }
    auto format(const data_dictionary::user_types_metadata& m, fmt::format_context& ctx) const
        -> decltype(ctx.out()) {
        return fmt::format_to(ctx.out(),
                              "org.apache.cassandra.config.UTMetaData@{}",
                              fmt::ptr(&m));
    }
};

auto fmt::formatter<data_dictionary::keyspace_metadata>::format(const data_dictionary::keyspace_metadata& m, fmt::format_context& ctx) const -> decltype(ctx.out()) {
    fmt::format_to(ctx.out(), "KSMetaData{{name={}, strategyClass={}, strategyOptions={}, cfMetaData={}, durable_writes={}, tablets=",
            m.name(), m.strategy_name(), m.strategy_options(), m.cf_meta_data(), m.durable_writes());
    if (m.initial_tablets()) {
        if (auto initial_tablets = m.initial_tablets().value()) {
            fmt::format_to(ctx.out(), "{{\"initial\":{}}}", initial_tablets);
        } else {
            fmt::format_to(ctx.out(), "{{\"enabled\":true}}");
        }
    } else {
        fmt::format_to(ctx.out(), "{{\"enabled\":false}}");
    }
    return fmt::format_to(ctx.out(), ", userTypes={}}}", m.user_types());
}

auto fmt::formatter<data_dictionary::storage_options>::format(const data_dictionary::storage_options& so, fmt::format_context& ctx) const -> decltype(ctx.out()) {
    return std::visit(overloaded_functor {
        [&ctx] (const data_dictionary::storage_options::local& so) -> decltype(ctx.out()) {
            return fmt::format_to(ctx.out(), "{}", so.dir);
        },
        [&ctx] (const data_dictionary::storage_options::s3& so) -> decltype(ctx.out()) {
            return std::visit(overloaded_functor {
                [&ctx, &so] (const sstring& prefix) -> decltype(ctx.out()) {
                    return fmt::format_to(ctx.out(), "s3://{}/{}", so.bucket, prefix);
                },
                [&ctx, &so] (const table_id& owner) -> decltype(ctx.out()) {
                    return fmt::format_to(ctx.out(), "s3://{} (owner {})", so.bucket, owner);
                }
            }, so.location);
        }
    }, so.value);
}
