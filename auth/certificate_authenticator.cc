/*
 * Copyright (C) 2022-present ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "auth/certificate_authenticator.hh"

#include <regex>
#include <fmt/ranges.h>

#include "utils/class_registrator.hh"
#include "utils/to_string.hh"
#include "data_dictionary/data_dictionary.hh"
#include "cql3/query_processor.hh"
#include "db/config.hh"

static const auto CERT_AUTH_NAME = "com.scylladb.auth.CertificateAuthenticator";
const std::string_view auth::certificate_authenticator_name(CERT_AUTH_NAME);

static logging::logger clogger("certificate_authenticator");

static const std::string cfg_source_attr = "source";
static const std::string cfg_query_attr = "query";

static const std::string cfg_source_subject = "SUBJECT";
static const std::string cfg_source_altname = "ALTNAME";

static const class_registrator<auth::authenticator
    , auth::certificate_authenticator
    , cql3::query_processor&
    , ::service::raft_group0_client&
    , ::service::migration_manager&> cert_auth_reg(CERT_AUTH_NAME);

enum class auth::certificate_authenticator::query_source {
    subject, altname
};

auth::certificate_authenticator::certificate_authenticator(cql3::query_processor& qp, ::service::raft_group0_client&, ::service::migration_manager&)
    : _queries([&] {
        auto& conf = qp.db().get_config();
        auto queries = conf.auth_certificate_role_queries();

        if (queries.empty()) {
            throw std::invalid_argument("No role extraction queries specified.");
        }

        std::vector<std::pair<query_source, boost::regex>> res;

        for (auto& map : queries) {
            // first, check for any invalid config keys
            if (map.size() == 2) {
                try {
                    auto& source = map.at(cfg_source_attr);
                    std::string query = map.at(cfg_query_attr);

                    std::transform(source.begin(), source.end(), source.begin(), ::toupper);

                    boost::regex ex(query);
                    if (ex.mark_count() != 1) {
                        throw std::invalid_argument("Role query must have exactly one mark expression");
                    }

                    clogger.debug("Append role query: {} : {}", source, query);

                    if (source == cfg_source_subject) {
                        res.emplace_back(query_source::subject, std::move(ex));
                    } else if (source == cfg_source_altname) {
                        res.emplace_back(query_source::altname, std::move(ex));
                    } else {
                        throw std::invalid_argument(fmt::format("Invalid source: {}", map.at(cfg_source_attr)));
                    }
                    continue;
                } catch (std::out_of_range&) {
                    // just fallthrough
                } catch (std::regex_error&) {
                    std::throw_with_nested(std::invalid_argument(fmt::format("Invalid query expression: {}", map.at(cfg_query_attr))));
                }
            }
            throw std::invalid_argument(fmt::format("Invalid query: {}", map));
        }
        return res;
    }())
{}

auth::certificate_authenticator::~certificate_authenticator() = default;

future<> auth::certificate_authenticator::start() {
    co_return;
}

future<> auth::certificate_authenticator::stop() {
    co_return;
}

std::string_view auth::certificate_authenticator::qualified_java_name() const {
    return certificate_authenticator_name;
}

bool auth::certificate_authenticator::require_authentication() const {
    return true;
}

auth::authentication_option_set auth::certificate_authenticator::supported_options() const {
    return {};
}

auth::authentication_option_set auth::certificate_authenticator::alterable_options() const {
    return {};
}

future<std::optional<auth::authenticated_user>> auth::certificate_authenticator::authenticate(session_dn_func f) const {
    if (!f) {
        co_return std::nullopt;
    }
    auto dninfo = co_await f();
    if (!dninfo) {
        throw exceptions::authentication_exception("No valid certificate found");
    }

    auto& subject = dninfo->subject;
    std::optional<std::string> altname ;

    const std::string* source_str = nullptr;

    for (auto& [source, expr] : _queries) {
        switch (source) {
            default:
            case query_source::subject:
                source_str = &subject;
                break;
            case query_source::altname:
                if (!altname) {
                    altname = dninfo->get_alt_names ? co_await dninfo->get_alt_names() : std::string{};
                }
                source_str = &*altname;
                break;
        }

        clogger.debug("Checking {}: {}", int(source), *source_str);

        boost::smatch m;
        if (boost::regex_search(*source_str, m, expr)) {
            auto username = m[1].str();
            clogger.debug("Return username: {}", username);
            co_return username;
        }
    }
    throw exceptions::authentication_exception(format("Subject '{}'/'{}' does not match any query expression", subject, altname));
}


future<auth::authenticated_user> auth::certificate_authenticator::authenticate(const credentials_map&) const {
    throw exceptions::authentication_exception("Cannot authenticate using attribute map");
}

future<> auth::certificate_authenticator::create(std::string_view role_name, const authentication_options& options, ::service::group0_batch& mc) {
    // TODO: should we keep track of roles/enforce existence? Role manager should deal with this...
    co_return;
}

future<> auth::certificate_authenticator::alter(std::string_view role_name, const authentication_options& options, ::service::group0_batch& mc) {
    co_return;
}

future<> auth::certificate_authenticator::drop(std::string_view role_name, ::service::group0_batch&) {
    co_return;
}

future<auth::custom_options> auth::certificate_authenticator::query_custom_options(std::string_view) const {
    co_return auth::custom_options{};
}

const auth::resource_set& auth::certificate_authenticator::protected_resources() const {
    static const resource_set resources;
    return resources;
}

::shared_ptr<auth::sasl_challenge> auth::certificate_authenticator::new_sasl_challenge() const {
    throw exceptions::authentication_exception("Login authentication not supported");
}
