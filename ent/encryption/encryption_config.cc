/*
 * Copyright (C) 2015 ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include <boost/filesystem.hpp>

#include "db/config.hh"
#include "utils/config_file_impl.hh"

#include "init.hh"
#include "encryption_config.hh"
#include "encryption.hh"

#include <fmt/ranges.h>

encryption::encryption_config::encryption_config()
                : config_file()
// BEGIN entry definitions

        , system_key_directory(this, "system_key_directory", value_status::Used, "/etc/scylla/conf/resources/system_keys",
                                R"foo(The directory where system keys are kept

This directory should have 700 permissions and belong to the scylla user)foo")

		, config_encryption_active(this, "config_encryption_active", value_status::Used, false, "")

        , config_encryption_key_name(this, "config_encryption_key_name", value_status::Used, "system_key",
                                "Set to the local encryption key filename or KMIP key URL to use for configuration file property value decryption")

        , system_info_encryption(this, "system_info_encryption", value_status::Used,
                                { { "enabled", "false" }, { CIPHER_ALGORITHM,
                                                "AES/CBC/PKCS5Padding" }, {
                                                SECRET_KEY_STRENGTH, "128" },
                                                },
                                R"foo(System information encryption settings

If enabled, system tables that may contain sensitive information (system.batchlog,
system.paxos), hints files and commit logs are encrypted with the
encryption settings below.

When enabling system table encryption on a node with existing data, run
`nodetool upgradesstables -a` on the listed tables to encrypt existing data.

When tracing is enabled, sensitive info will be written into the tables in the
system_traces keyspace. Those tables should be configured to encrypt their data
on disk.

It is recommended to use remote encryption keys from a KMIP server when using 
Transparent Data Encryption (TDE) features.
Local key support is provided when a KMIP server is not available.

See the scylla documentation for available key providers and their properties.
)foo")
		, kmip_hosts(this, "kmip_hosts", value_status::Used, { },
                                R"foo(KMIP host(s). 

The unique name of kmip host/cluster that can be referenced in table schema.

host.yourdomain.com={ hosts=<host1[:port]>[, <host2[:port]>...], keyfile=/path/to/keyfile, truststore=/path/to/truststore.pem, key_cache_millis=<cache ms>, timeout=<timeout ms> }:...

The KMIP connection management only supports failover, so all requests will go through a 
single KMIP server. There is no load balancing, as no KMIP servers (at the time of this writing)
support read replication, or other strategies for availability.

Hosts are tried in the order they appear here. Add them in the same sequence they'll fail over in.

KMIP requests will fail over/retry 'max_command_retries' times (default 3)

)foo")
		  , kms_hosts(this, "kms_hosts", value_status::Used, { },
                                R"foo(KMS host(s). 

The unique name of kms host that can be referenced in table schema.

host.yourdomain.com={ endpoint=<http(s)://host[:port]>, aws_access_key_id=<AWS access id>, aws_secret_access_key=<AWS secret key>, aws_profile<profile>, aws_region=<AWS region>, aws_use_ec2_credentials<bool>, aws_use_ec2_region=<bool>, aws_assume_role_arn=<AWS role arn>, master_key=<alias or id>, keyfile=/path/to/keyfile, truststore=/path/to/truststore.pem, key_cache_millis=<cache ms>, timeout=<timeout ms> }:...

Actual connection can be either an explicit endpoint (<host>:<port>), or selected automatic via aws_region.

If aws_use_ec2_region is true, regions is instead queried from EC2 metadata.

Authentication can be explicit with aws_access_key_id and aws_secret_access_key. Either secret or both can be omitted
in which case the provider will try to read them from AWS credentials in ~/.aws/credentials

If aws_use_ec2_credentials is true, authentication is instead queried from EC2 metadata.

If aws_assume_role_arn is set, scylla will issue an AssumeRole command and use the resulting security token for key operations.

master_key is an AWS KMS key id or alias from which all keys used for actual encryption of scylla data will be derived.
This key must be pre-created with access policy allowing the above AWS id Encrypt, Decrypt and GenerateDataKey operations.

)foo")
		  , gcp_hosts(this, "gcp_hosts", value_status::Used, { },
                                R"foo(Google Compute Engine KMS host(s). 

The unique name of GCP kms host that can be referenced in table schema.

gcp_project_id=<GCP project>, gcp_location=<project location>, master_key=<existing key to use for encrypting data keys>, gcp_credentials_file=<credentials json file>, gcp_impersonate_service_account=<impersonate this account for all KMS operations>,keyfile=/path/to/keyfile, truststore=/path/to/truststore.pem, key_cache_millis=<cache ms>, timeout=<timeout ms> }:...

Authentication can be explicit with auth_file or by resolving default credentials (see google docs). 

If use_gcp_machine_credentials is true, authentication is instead queried from GCP metadata.

auth_file can contain either a user, service or impersonated service account.

master_key is an GCP KMS key name from which all keys used for actual encryption of scylla data will be derived.
This key must be pre-created with access policy allowing the above credentials Encrypt and Decrypt operations.

)foo")
        , user_info_encryption(this, "user_info_encryption", value_status::Used,
                                { { "enabled", "false" }, { CIPHER_ALGORITHM,
                                                "AES/CBC/PKCS5Padding" }, {
                                                SECRET_KEY_STRENGTH, "128" },
                                                },
                                R"foo(Global user table encryption settings. If enabled, all user tables
                                will be encrypted using the provided settings, unless overridden
                                by table scylla_encryption_options.)foo")
        , allow_per_table_encryption(this, "allow_per_table_encryption", value_status::Used, true,
            "If 'user_info_encryption` is enabled this controls whether specifying per-table encryption using create/alter table is allowed"
        )


// END entry definitions
{}

static class : public configurable {
    std::unordered_map<const db::config*, std::unique_ptr<encryption::encryption_config>> _cfgs;

public:
    void append_options(db::config& cfg, boost::program_options::options_description_easy_init& init) override {
        // While it is fine for normal execution to have just one, static (us) encryption config, 
        // it does not work well with unit testing, where we repeatedly create new cql_test_envs etc,
        // since new config values will not be overwritten due to the actual named_values being shared here.
        // Fix this (temporarily) by simply keeping a local map cfg->ecfg and using these.
        // TODO: improve this by allowing db::config to hold named sub->configs (mapping config file objects).
        if (_cfgs.count(&cfg)) {
            throw std::runtime_error("Config already processed");
        }
        auto& ccfg = _cfgs.emplace(&cfg, std::make_unique<encryption::encryption_config>()).first->second;
        // hook into main scylla.yaml.
        cfg.add(ccfg->values());
    }
    future<notify_func> initialize_ex(const boost::program_options::variables_map& opts, const db::config& cfg, db::extensions& exts, const service_set& services) override {
        auto ccfg = _cfgs.count(&cfg) ? std::move(_cfgs.at(&cfg)) : std::make_unique<encryption::encryption_config>();
        _cfgs.erase(&cfg);
        auto ctxt = co_await encryption::register_extensions(cfg, std::move(ccfg), exts, services);
        co_return [ctxt](system_state e) -> future<> {
            switch (e) {
                case system_state::started:
                    co_await ctxt->start();
                    break;
                case system_state::stopped:
                    co_await ctxt->stop();
                    break;
                default:
                    break;
            }
        };
    }
} cfg;
