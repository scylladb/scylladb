/*
 *
 * Modified by ScyllaDB
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

#include <seastar/core/sstring.hh>
#include "utils/serialization.hh"
#include "locator/host_id.hh"
#include "version_generator.hh"
#include "gms/inet_address.hh"
#include "dht/token.hh"
#include "schema/schema_fwd.hh"
#include "version.hh"
#include "cdc/generation_id.hh"
#include <unordered_set>

namespace gms {

/**
 * This abstraction represents the state associated with a particular node which an
 * application wants to make available to the rest of the nodes in the cluster.
 * Whenever a piece of state needs to be disseminated to the rest of cluster wrap
 * the state in an instance of <i>ApplicationState</i> and add it to the Gossiper.
 * <p></p>
 * e.g. if we want to disseminate load information for node A do the following:
 * <p></p>
 * ApplicationState loadState = new ApplicationState(<string representation of load>);
 * Gossiper.instance.addApplicationState("LOAD STATE", loadState);
 */

class versioned_value {
    version_type _version;
    sstring _value;
public:
    // this must be a char that cannot be present in any token
    static constexpr char DELIMITER = ',';
    static constexpr const char DELIMITER_STR[] = { DELIMITER, 0 };

    // values for ApplicationState.STATUS
    static constexpr const char* STATUS_UNKNOWN = "UNKNOWN";
    static constexpr const char* STATUS_BOOTSTRAPPING = "BOOT";
    static constexpr const char* STATUS_NORMAL = "NORMAL";
    static constexpr const char* STATUS_LEFT = "LEFT";

    static constexpr const char* REMOVED_TOKEN = "removed";

    static constexpr const char* SHUTDOWN = "shutdown";

    version_type version() const noexcept { return _version; };
    const sstring& value() const noexcept { return _value; };
public:
    bool operator==(const versioned_value& other) const noexcept {
        return _version == other._version &&
               _value   == other._value;
    }

public:
    versioned_value(const sstring& value, version_type version = version_generator::get_next_version())
        : _version(version), _value(value) {
#if 0
        // blindly interning everything is somewhat suboptimal -- lots of VersionedValues are unique --
        // but harmless, and interning the non-unique ones saves significant memory.  (Unfortunately,
        // we don't really have enough information here in VersionedValue to tell the probably-unique
        // values apart.)  See CASSANDRA-6410.
        this.value = value.intern();
#endif
    }

    versioned_value(sstring&& value, version_type version = version_generator::get_next_version()) noexcept
        : _version(version), _value(std::move(value)) {
    }

    versioned_value() noexcept
        : _version(-1) {
    }

    static sstring version_string(const std::initializer_list<sstring>& args) {
        return fmt::to_string(fmt::join(args, std::string_view(versioned_value::DELIMITER_STR)));
    }

    static sstring make_full_token_string(const std::unordered_set<dht::token>& tokens);
    static sstring make_token_string(const std::unordered_set<dht::token>& tokens);
    static sstring make_cdc_generation_id_string(std::optional<cdc::generation_id>);

    // Reverse of `make_full_token_string`.
    static std::unordered_set<dht::token> tokens_from_string(const sstring&);

    // Reverse of `make_cdc_generation_id_string`.
    static std::optional<cdc::generation_id> cdc_generation_id_from_string(const sstring&);

    static versioned_value clone_with_higher_version(const versioned_value& value) noexcept {
        return versioned_value(value.value());
    }

    static versioned_value bootstrapping(const std::unordered_set<dht::token>& tokens) {
        return versioned_value(version_string({sstring(versioned_value::STATUS_BOOTSTRAPPING),
                                               make_token_string(tokens)}));
    }

    static versioned_value normal(const std::unordered_set<dht::token>& tokens) {
        return versioned_value(version_string({sstring(versioned_value::STATUS_NORMAL),
                                               make_token_string(tokens)}));
    }

    static versioned_value load(double load) {
        return versioned_value(to_sstring(load));
    }

    static versioned_value schema(const table_schema_version& new_version) {
        return versioned_value(new_version.to_sstring());
    }

    static versioned_value left(const std::unordered_set<dht::token>& tokens, int64_t expire_time) {
        return versioned_value(version_string({sstring(versioned_value::STATUS_LEFT),
                                               make_token_string(tokens),
                                               std::to_string(expire_time)}));
    }

    static versioned_value host_id(const locator::host_id& host_id) {
        return versioned_value(host_id.to_sstring());
    }

    static versioned_value tokens(const std::unordered_set<dht::token>& tokens) {
        return versioned_value(make_full_token_string(tokens));
    }

    static versioned_value cdc_generation_id(std::optional<cdc::generation_id> gen_id) {
        return versioned_value(make_cdc_generation_id_string(gen_id));
    }

    static versioned_value removed_nonlocal(const locator::host_id& host_id, int64_t expire_time) {
        return versioned_value(sstring(REMOVED_TOKEN) + sstring(DELIMITER_STR) +
            host_id.to_sstring() + sstring(DELIMITER_STR) + to_sstring(expire_time));
    }

    static versioned_value shutdown(bool value) {
        return versioned_value(sstring(SHUTDOWN) + sstring(DELIMITER_STR) + (value ? "true" : "false"));
    }

    static versioned_value datacenter(const sstring& dc_id) {
        return versioned_value(dc_id);
    }

    static versioned_value rack(const sstring& rack_id) {
        return versioned_value(rack_id);
    }

    static versioned_value snitch_name(const sstring& snitch_name) {
        return versioned_value(snitch_name);
    }

    static versioned_value shard_count(int shard_count) {
        return versioned_value(format("{}", shard_count));
    }

    static versioned_value ignore_msb_bits(unsigned ignore_msb_bits) {
        return versioned_value(format("{}", ignore_msb_bits));
    }

    static versioned_value rpcaddress(gms::inet_address endpoint) {
        return versioned_value(format("{}", endpoint));
    }

    static versioned_value release_version() {
        return versioned_value(version::release());
    }

    static versioned_value network_version();

    static versioned_value internal_ip(const sstring &private_ip) {
        return versioned_value(private_ip);
    }

    static versioned_value severity(double value) {
        return versioned_value(to_sstring(value));
    }

    static versioned_value supported_features(const std::set<std::string_view>& features) {
        return versioned_value(fmt::to_string(fmt::join(features, ",")));
    }

    static versioned_value cache_hitrates(const sstring& hitrates) {
        return versioned_value(hitrates);
    }

    static versioned_value cql_ready(bool value) {
        return versioned_value(to_sstring(int(value)));
    };
}; // class versioned_value

} // namespace gms

template <> struct fmt::formatter<gms::versioned_value> : fmt::formatter<string_view> {
    auto format(const gms::versioned_value& v, fmt::format_context& ctx) const {
        return fmt::format_to(ctx.out(), "Value({},{})", v.value(), v.version());
    }
};
