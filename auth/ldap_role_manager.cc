/*
 * Copyright (C) 2019 ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "ldap_role_manager.hh"

#include <boost/algorithm/string/replace.hpp>
#include <fmt/format.h>
#include <fmt/ranges.h>
#include <ldap.h>
#include <seastar/core/seastar.hh>
#include <seastar/core/sstring.hh>
#include <seastar/net/dns.hh>
#include <seastar/util/log.hh>
#include <seastar/core/coroutine.hh>
#include <vector>

#include "common.hh"
#include "cql3/query_processor.hh"
#include "exceptions/exceptions.hh"
#include "seastarx.hh"
#include "service/raft/raft_group0_client.hh"
#include "utils/class_registrator.hh"
#include "db/config.hh"
#include "utils/exponential_backoff_retry.hh"

namespace {

logger mylog{"ldap_role_manager"}; // `log` is taken by math.

struct url_desc_deleter {
    void operator()(LDAPURLDesc *p) {
        ldap_free_urldesc(p);
    }
};

using url_desc_ptr = std::unique_ptr<LDAPURLDesc, url_desc_deleter>;

url_desc_ptr parse_url(std::string_view url) {
    LDAPURLDesc *desc = nullptr;
    if (ldap_url_parse(url.data(), &desc)) {
        mylog.error("error in ldap_url_parse({})", url);
    }
    return url_desc_ptr(desc);
}

/// Extracts attribute \p attr from all entries in \p res.
std::vector<sstring> get_attr_values(LDAP* ld, LDAPMessage* res, const char* attr) {
    std::vector<sstring> values;
    mylog.debug("Analyzing search results");
    for (auto e = ldap_first_entry(ld, res); e; e = ldap_next_entry(ld, e)) {
        struct deleter {
            void operator()(berval** p) { ldap_value_free_len(p); }
            void operator()(char* p) { ldap_memfree(p); }
        };
        const std::unique_ptr<char, deleter> dname(ldap_get_dn(ld, e));
        mylog.debug("Analyzing entry {}", dname.get());
        const std::unique_ptr<berval*, deleter> vals(ldap_get_values_len(ld, e, attr));
        if (!vals) {
            mylog.warn("LDAP entry {} has no attribute {}", dname.get(), attr);
            continue;
        }
        for (size_t i = 0; vals.get()[i]; ++i) {
            values.emplace_back(vals.get()[i]->bv_val, vals.get()[i]->bv_len);
        }
    }
    mylog.debug("Done analyzing search results; extracted roles {}", values);
    return values;
}

const char* ldap_role_manager_full_name = "com.scylladb.auth.LDAPRoleManager";

} // anonymous namespace

namespace auth {

static const class_registrator<
    role_manager,
    ldap_role_manager,
    cql3::query_processor&,
    ::service::raft_group0_client&,
    ::service::migration_manager&> registration(ldap_role_manager_full_name);

ldap_role_manager::ldap_role_manager(
        std::string_view query_template, std::string_view target_attr, std::string_view bind_name, std::string_view bind_password,
        cql3::query_processor& qp, ::service::raft_group0_client& rg0c, ::service::migration_manager& mm)
        : _std_mgr(qp, rg0c, mm), _group0_client(rg0c), _query_template(query_template), _target_attr(target_attr), _bind_name(bind_name)
        , _bind_password(bind_password)
        , _connection_factory(bind(std::mem_fn(&ldap_role_manager::reconnect), std::ref(*this))) {
}

ldap_role_manager::ldap_role_manager(cql3::query_processor& qp, ::service::raft_group0_client& rg0c, ::service::migration_manager& mm)
    : ldap_role_manager(
            qp.db().get_config().ldap_url_template(),
            qp.db().get_config().ldap_attr_role(),
            qp.db().get_config().ldap_bind_dn(),
            qp.db().get_config().ldap_bind_passwd(),
            qp,
            rg0c,
            mm) {
}

std::string_view ldap_role_manager::qualified_java_name() const noexcept {
    return ldap_role_manager_full_name;
}

const resource_set& ldap_role_manager::protected_resources() const {
    return _std_mgr.protected_resources();
}

future<> ldap_role_manager::start() {
    if (!parse_url(get_url("dummy-user"))) { // Just need host and port -- any user should do.
        return make_exception_future(
                std::runtime_error(fmt::format("error getting LDAP server address from template {}", _query_template)));
    }
    return _std_mgr.start();
}

using conn_ptr = lw_shared_ptr<ldap_connection>;

future<conn_ptr> ldap_role_manager::connect() {
    const auto desc = parse_url(get_url("dummy-user")); // Just need host and port -- any user should do.
    if (!desc) {
        co_return coroutine::exception(std::make_exception_ptr(std::runtime_error("connect attempted before a successful start")));
    }
    net::inet_address host = co_await net::dns::resolve_name(desc->lud_host);
    const socket_address addr(host, uint16_t(desc->lud_port));
    connected_socket sock = co_await seastar::connect(addr);
    auto conn = make_lw_shared<ldap_connection>(std::move(sock));
    sstring error;
    try {
        ldap_msg_ptr response = co_await conn->simple_bind(_bind_name.c_str(), _bind_password.c_str());
        if (!response || ldap_msgtype(response.get()) != LDAP_RES_BIND) {
            error = format("simple_bind error: {}", conn->get_error());
        }
    } catch (...) {
        error = format("connect error: {}", std::current_exception());
    }
    if (!error.empty()) {
        co_await conn->close();
        co_return coroutine::exception(std::make_exception_ptr(std::runtime_error(std::move(error))));
    }
    co_return std::move(conn);
}

future<conn_ptr> ldap_role_manager::reconnect() {
    unsigned retries_left = 5;
    using namespace std::literals::chrono_literals;
    conn_ptr conn = co_await exponential_backoff_retry::do_until_value(1s, 32s, _as, [this, &retries_left] () -> future<std::optional<conn_ptr>> {
        if (!retries_left) {
            co_return conn_ptr{};
        }
        mylog.trace("reconnect() retrying ({} attempts left)", retries_left);
        --retries_left;
        try {
            co_return co_await connect();
        } catch (...) {
            mylog.error("error in reconnect: {}", std::current_exception());
        }
        co_return std::nullopt;
    });

    mylog.trace("reconnect() finished backoff, conn={}", reinterpret_cast<void*>(conn.get()));
    if (conn) {
        co_return std::move(conn);
    }
    co_return coroutine::exception(std::make_exception_ptr(std::runtime_error("reconnect failed after 5 attempts")));
}

future<> ldap_role_manager::stop() {
    _as.request_abort();
    return _std_mgr.stop().then([this] { return _connection_factory.stop(); });
}

future<> ldap_role_manager::create(std::string_view name, const role_config& config, ::service::group0_batch& mc) {
    return _std_mgr.create(name, config, mc);
}

future<> ldap_role_manager::drop(std::string_view name, ::service::group0_batch& mc) {
    return _std_mgr.drop(name, mc);
}

future<> ldap_role_manager::alter(std::string_view name, const role_config_update& config, ::service::group0_batch& mc) {
    return _std_mgr.alter(name, config, mc);
}

future<> ldap_role_manager::grant(std::string_view, std::string_view, ::service::group0_batch& mc) {
    return make_exception_future<>(exceptions::invalid_request_exception("Cannot grant roles with LDAPRoleManager."));
}

future<> ldap_role_manager::revoke(std::string_view, std::string_view, ::service::group0_batch& mc) {
    return make_exception_future<>(exceptions::invalid_request_exception("Cannot revoke roles with LDAPRoleManager."));
}

future<role_set> ldap_role_manager::query_granted(std::string_view grantee_name, recursive_role_query) {
    const auto url = get_url(grantee_name.data());
    auto desc = parse_url(url);
    if (!desc) {
        return make_exception_future<role_set>(std::runtime_error(format("Error parsing URL {}", url)));
    }
    return _connection_factory.with_connection([this, desc = std::move(desc), grantee_name_ = sstring(grantee_name)]
                                               (ldap_connection& conn) -> future<role_set> {
        sstring grantee_name = std::move(grantee_name_);
        ldap_msg_ptr res = co_await conn.search(desc->lud_dn, desc->lud_scope, desc->lud_filter, desc->lud_attrs,
                           /*attrsonly=*/0, /*serverctrls=*/nullptr, /*clientctrls=*/nullptr,
                           /*timeout=*/nullptr, /*sizelimit=*/0);
        mylog.trace("query_granted: got search results");
        const auto mtype = ldap_msgtype(res.get());
        if (mtype != LDAP_RES_SEARCH_ENTRY && mtype != LDAP_RES_SEARCH_RESULT && mtype != LDAP_RES_SEARCH_REFERENCE) {
            mylog.error("ldap search yielded result {} of type {}", static_cast<const void*>(res.get()), mtype);
            co_return coroutine::exception(std::make_exception_ptr(std::runtime_error("ldap_role_manager: search result has wrong type")));
        }
        std::vector<sstring> values = get_attr_values(conn.get_ldap(), res.get(), _target_attr.c_str());
        auth::role_set valid_roles{grantee_name};

        // Each value is a role to be granted.
        co_await parallel_for_each(values, [this, &valid_roles] (const sstring& ldap_role) {
            return _std_mgr.exists(ldap_role).then([&valid_roles, &ldap_role] (bool exists) {
                if (exists) {
                    valid_roles.insert(ldap_role);
                } else {
                    mylog.error("unrecognized role received from LDAP: {}", ldap_role);
                }
            });
        });

        co_return std::move(valid_roles);
    });
}

future<role_to_directly_granted_map>
ldap_role_manager::query_all_directly_granted() {
    role_to_directly_granted_map result;
    auto roles = co_await query_all();
    for (auto& role: roles) {
        auto granted_set = co_await query_granted(role, recursive_role_query::no);
        for (auto& granted: granted_set) {
            if (granted != role) {
                result.insert({role, granted});
            }
        }
    }
    co_return result;
}

future<role_set> ldap_role_manager::query_all() {
    return _std_mgr.query_all();
}

future<> ldap_role_manager::create_role(std::string_view role_name) {
    return smp::submit_to(0, [this, role_name] () -> future<> {
        int retries = 10;
        while (true) {
            auto guard = co_await _group0_client.start_operation(_as, ::service::raft_timeout{});
            ::service::group0_batch batch(std::move(guard));
            auto cfg = role_config{.can_login = true};
            try {
                co_await create(role_name, cfg, batch);
                co_await std::move(batch).commit(_group0_client, _as, ::service::raft_timeout{});
            } catch (const role_already_exists&) {
                // ok
            } catch (const ::service::group0_concurrent_modification& ex) {
                mylog.warn("Failed to auto-create role \"{}\" due to guard conflict.{}.",
                        role_name, retries ? " Retrying" : " Number of retries exceeded, giving up");
                if (retries--) {
                    continue;
                }
                throw;
            }
            break;
        }
        // make sure to wait until create mutations are applied locally
        (void)(co_await _group0_client.start_operation(_as, ::service::raft_timeout{}));
    });
}

future<bool> ldap_role_manager::exists(std::string_view role_name) {
    bool exists = co_await _std_mgr.exists(role_name);
    if (exists) {
        co_return true;
    }
    role_set roles = co_await query_granted(role_name, recursive_role_query::yes);
    // A role will get auto-created if it's already assigned any permissions.
    // The role set will always contains at least a single entry (the role itself),
    // so auto-creation is only triggered if at least one more external role is assigned.
    if (roles.size() > 1) {
        mylog.info("Auto-creating user {}", role_name);
        try {
            co_await create_role(role_name);
            exists = true;
        } catch (...) {
            mylog.error("Failed to auto-create role {}: {}", role_name, std::current_exception());
            exists = false;
        }
        co_return exists;
    }
    mylog.debug("Role {} will not be auto-created", role_name);
    co_return false;
}

future<bool> ldap_role_manager::is_superuser(std::string_view role_name) {
    return _std_mgr.is_superuser(role_name);
}

future<bool> ldap_role_manager::can_login(std::string_view role_name) {
    return _std_mgr.can_login(role_name);
}

future<std::optional<sstring>> ldap_role_manager::get_attribute(
        std::string_view role_name, std::string_view attribute_name) {
    return _std_mgr.get_attribute(role_name, attribute_name);
}

future<role_manager::attribute_vals> ldap_role_manager::query_attribute_for_all(std::string_view attribute_name) {
    return _std_mgr.query_attribute_for_all(attribute_name);
}

future<> ldap_role_manager::set_attribute(
        std::string_view role_name, std::string_view attribute_name, std::string_view attribute_value, ::service::group0_batch& mc) {
    return _std_mgr.set_attribute(role_name, attribute_value, attribute_value, mc);
}

future<> ldap_role_manager::remove_attribute(std::string_view role_name, std::string_view attribute_name, ::service::group0_batch& mc) {
    return _std_mgr.remove_attribute(role_name, attribute_name, mc);
}

sstring ldap_role_manager::get_url(std::string_view user) const {
    return boost::replace_all_copy(_query_template, "{USER}", user);
}

future<std::vector<cql3::description>> ldap_role_manager::describe_role_grants() {
    // Since grants are performed by the ldap admin, we shouldn't echo them back
    co_return std::vector<cql3::description>();
}

future<> ldap_role_manager::ensure_superuser_is_created() {
    // ldap is responsible for users
    co_return;
}

} // namespace auth
