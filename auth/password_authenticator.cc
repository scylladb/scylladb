/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Copyright (C) 2016 ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

#include <unistd.h>
#include <crypt.h>
#include <random>
#include <chrono>

#include <seastar/core/reactor.hh>

#include "common.hh"
#include "password_authenticator.hh"
#include "authenticated_user.hh"
#include "cql3/untyped_result_set.hh"
#include "log.hh"
#include "service/migration_manager.hh"
#include "utils/class_registrator.hh"

const sstring& auth::password_authenticator_name() {
    static const sstring name = meta::AUTH_PACKAGE_NAME + "PasswordAuthenticator";
    return name;
}

// name of the hash column.
static const sstring SALTED_HASH = "salted_hash";
static const sstring USER_NAME = "username";
static const sstring DEFAULT_USER_NAME = auth::meta::DEFAULT_SUPERUSER_NAME;
static const sstring DEFAULT_USER_PASSWORD = auth::meta::DEFAULT_SUPERUSER_NAME;
static const sstring CREDENTIALS_CF = "credentials";

static logging::logger plogger("password_authenticator");

// To ensure correct initialization order, we unfortunately need to use a string literal.
static const class_registrator<
        auth::authenticator,
        auth::password_authenticator,
        cql3::query_processor&,
        ::service::migration_manager&> password_auth_reg("org.apache.cassandra.auth.PasswordAuthenticator");

auth::password_authenticator::~password_authenticator()
{}

auth::password_authenticator::password_authenticator(cql3::query_processor& qp, ::service::migration_manager& mm)
    : _qp(qp)
    , _migration_manager(mm) {
}

// TODO: blowfish
// Origin uses Java bcrypt library, i.e. blowfish salt
// generation and hashing, which is arguably a "better"
// password hash than sha/md5 versions usually available in
// crypt_r. Otoh, glibc 2.7+ uses a modified sha512 algo
// which should be the same order of safe, so the only
// real issue should be salted hash compatibility with
// origin if importing system tables from there.
//
// Since bcrypt/blowfish is _not_ (afaict) not available
// as a dev package/lib on most linux distros, we'd have to
// copy and compile for example OWL  crypto
// (http://cvsweb.openwall.com/cgi/cvsweb.cgi/Owl/packages/glibc/crypt_blowfish/)
// to be fully bit-compatible.
//
// Until we decide this is needed, let's just use crypt_r,
// and some old-fashioned random salt generation.

static constexpr size_t rand_bytes = 16;
static thread_local crypt_data tlcrypt = { 0, };

static sstring hashpw(const sstring& pass, const sstring& salt) {
    auto res = crypt_r(pass.c_str(), salt.c_str(), &tlcrypt);
    if (res == nullptr) {
        throw std::system_error(errno, std::system_category());
    }
    return res;
}

static bool checkpw(const sstring& pass, const sstring& salted_hash) {
    auto tmp = hashpw(pass, salted_hash);
    return tmp == salted_hash;
}

static sstring gensalt() {
    static sstring prefix;

    std::random_device rd;
    std::default_random_engine e1(rd());
    std::uniform_int_distribution<char> dist;

    sstring valid_salt = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789./";
    sstring input(rand_bytes, 0);

    for (char&c : input) {
        c = valid_salt[dist(e1) % valid_salt.size()];
    }

    sstring salt;

    if (!prefix.empty()) {
        return prefix + input;
    }

    // Try in order:
    // blowfish 2011 fix, blowfish, sha512, sha256, md5
    for (sstring pfx : { "$2y$", "$2a$", "$6$", "$5$", "$1$" }) {
        salt = pfx + input;
        if (crypt_r("fisk", salt.c_str(), &tlcrypt)) {
            prefix = pfx;
            return salt;
        }
    }
    throw std::runtime_error("Could not initialize hashing algorithm");
}

static sstring hashpw(const sstring& pass) {
    return hashpw(pass, gensalt());
}

future<> auth::password_authenticator::start() {
    return auth::once_among_shards([this] {
        gensalt(); // do this once to determine usable hashing

        static const sstring create_table = sprint(
                "CREATE TABLE %s.%s ("
                "%s text,"
                "%s text," // salt + hash + number of rounds
                "options map<text,text>,"// for future extensions
                "PRIMARY KEY(%s)"
                ") WITH gc_grace_seconds=%d",
                meta::AUTH_KS,
                CREDENTIALS_CF, USER_NAME, SALTED_HASH, USER_NAME,
                90 * 24 * 60 * 60); // 3 months.

        return auth::create_metadata_table_if_missing(
                CREDENTIALS_CF,
                _qp,
                create_table,
                _migration_manager).then([this] {
            auth::delay_until_system_ready(_delayed, [this] {
                return has_existing_users().then([this](bool existing) {
                    if (!existing) {
                        return _qp.process(
                                sprint(
                                        "INSERT INTO %s.%s (%s, %s) VALUES (?, ?) USING TIMESTAMP 0",
                                        meta::AUTH_KS,
                                        CREDENTIALS_CF,
                                        USER_NAME, SALTED_HASH),
                                db::consistency_level::ONE,
                                { DEFAULT_USER_NAME, hashpw(DEFAULT_USER_PASSWORD) }).then([](auto) {
                            plogger.info("Created default user '{}'", DEFAULT_USER_NAME);
                        });
                    }

                    return make_ready_future<>();
                });
            });
        });
    });
}

future<> auth::password_authenticator::stop() {
    return make_ready_future<>();
}

db::consistency_level auth::password_authenticator::consistency_for_user(const sstring& username) {
    if (username == DEFAULT_USER_NAME) {
        return db::consistency_level::QUORUM;
    }
    return db::consistency_level::LOCAL_ONE;
}

const sstring& auth::password_authenticator::qualified_java_name() const {
    return password_authenticator_name();
}

bool auth::password_authenticator::require_authentication() const {
    return true;
}

auth::authenticator::option_set auth::password_authenticator::supported_options() const {
    return option_set::of<option::PASSWORD>();
}

auth::authenticator::option_set auth::password_authenticator::alterable_options() const {
    return option_set::of<option::PASSWORD>();
}

future<::shared_ptr<auth::authenticated_user> > auth::password_authenticator::authenticate(
                const credentials_map& credentials) const {
    if (!credentials.count(USERNAME_KEY)) {
        throw exceptions::authentication_exception(sprint("Required key '%s' is missing", USERNAME_KEY));
    }
    if (!credentials.count(PASSWORD_KEY)) {
        throw exceptions::authentication_exception(sprint("Required key '%s' is missing", PASSWORD_KEY));
    }

    auto& username = credentials.at(USERNAME_KEY);
    auto& password = credentials.at(PASSWORD_KEY);

    // Here was a thread local, explicit cache of prepared statement. In normal execution this is
    // fine, but since we in testing set up and tear down system over and over, we'd start using
    // obsolete prepared statements pretty quickly.
    // Rely on query processing caching statements instead, and lets assume
    // that a map lookup string->statement is not gonna kill us much.
    return futurize_apply([this, username, password] {
        return _qp.process(sprint("SELECT %s FROM %s.%s WHERE %s = ?", SALTED_HASH,
                                        meta::AUTH_KS, CREDENTIALS_CF, USER_NAME),
                        consistency_for_user(username), {username}, true);
    }).then_wrapped([=](future<::shared_ptr<cql3::untyped_result_set>> f) {
        try {
            auto res = f.get0();
            if (res->empty() || !checkpw(password, res->one().get_as<sstring>(SALTED_HASH))) {
                throw exceptions::authentication_exception("Username and/or password are incorrect");
            }
            return make_ready_future<::shared_ptr<authenticated_user>>(::make_shared<authenticated_user>(username));
        } catch (std::system_error &) {
            std::throw_with_nested(exceptions::authentication_exception("Could not verify password"));
        } catch (exceptions::request_execution_exception& e) {
            std::throw_with_nested(exceptions::authentication_exception(e.what()));
        } catch (...) {
            std::throw_with_nested(exceptions::authentication_exception("authentication failed"));
        }
    });
}

future<> auth::password_authenticator::create(sstring username,
                const option_map& options) {
    try {
        auto password = boost::any_cast<sstring>(options.at(option::PASSWORD));
        auto query = sprint("INSERT INTO %s.%s (%s, %s) VALUES (?, ?)",
                        meta::AUTH_KS, CREDENTIALS_CF, USER_NAME, SALTED_HASH);
        return _qp.process(query, consistency_for_user(username), { username, hashpw(password) }).discard_result();
    } catch (std::out_of_range&) {
        throw exceptions::invalid_request_exception("PasswordAuthenticator requires PASSWORD option");
    }
}

future<> auth::password_authenticator::alter(sstring username,
                const option_map& options) {
    try {
        auto password = boost::any_cast<sstring>(options.at(option::PASSWORD));
        auto query = sprint("UPDATE %s.%s SET %s = ? WHERE %s = ?",
                        meta::AUTH_KS, CREDENTIALS_CF, SALTED_HASH, USER_NAME);
        return _qp.process(query, consistency_for_user(username), { hashpw(password), username }).discard_result();
    } catch (std::out_of_range&) {
        throw exceptions::invalid_request_exception("PasswordAuthenticator requires PASSWORD option");
    }
}

future<> auth::password_authenticator::drop(sstring username) {
    try {
        auto query = sprint("DELETE FROM %s.%s WHERE %s = ?",
                        meta::AUTH_KS, CREDENTIALS_CF, USER_NAME);
        return _qp.process(query, consistency_for_user(username), { username }).discard_result();
    } catch (std::out_of_range&) {
        throw exceptions::invalid_request_exception("PasswordAuthenticator requires PASSWORD option");
    }
}

const auth::resource_ids& auth::password_authenticator::protected_resources() const {
    static const resource_ids ids({ data_resource(meta::AUTH_KS, CREDENTIALS_CF) });
    return ids;
}

::shared_ptr<auth::authenticator::sasl_challenge> auth::password_authenticator::new_sasl_challenge() const {
    class plain_text_password_challenge: public sasl_challenge {
        const password_authenticator& _self;

    public:
        plain_text_password_challenge(const password_authenticator& self) : _self(self)
        {}

        /**
         * SASL PLAIN mechanism specifies that credentials are encoded in a
         * sequence of UTF-8 bytes, delimited by 0 (US-ASCII NUL).
         * The form is : {code}authzId<NUL>authnId<NUL>password<NUL>{code}
         * authzId is optional, and in fact we don't care about it here as we'll
         * set the authzId to match the authnId (that is, there is no concept of
         * a user being authorized to act on behalf of another).
         *
         * @param bytes encoded credentials string sent by the client
         * @return map containing the username/password pairs in the form an IAuthenticator
         * would expect
         * @throws javax.security.sasl.SaslException
         */
        bytes evaluate_response(bytes_view client_response) override {
            plogger.debug("Decoding credentials from client token");

            sstring username, password;

            auto b = client_response.crbegin();
            auto e = client_response.crend();
            auto i = b;

            while (i != e) {
                if (*i == 0) {
                    sstring tmp(i.base(), b.base());
                    if (password.empty()) {
                        password = std::move(tmp);
                    } else if (username.empty()) {
                        username = std::move(tmp);
                    }
                    b = ++i;
                    continue;
                }
                ++i;
            }

            if (username.empty()) {
                throw exceptions::authentication_exception("Authentication ID must not be null");
            }
            if (password.empty()) {
                throw exceptions::authentication_exception("Password must not be null");
            }

            _credentials[USERNAME_KEY] = std::move(username);
            _credentials[PASSWORD_KEY] = std::move(password);
            _complete = true;
            return {};
        }
        bool is_complete() const override {
            return _complete;
        }
        future<::shared_ptr<authenticated_user>> get_authenticated_user() const override {
            return _self.authenticate(_credentials);
        }
    private:
        credentials_map _credentials;
        bool _complete = false;
    };
    return ::make_shared<plain_text_password_challenge>(*this);
}


//
// Similar in structure to `auth::service::has_existing_users()`, but trying to generalize the pattern breaks all kinds
// of module boundaries and leaks implementation details.
//
future<bool> auth::password_authenticator::has_existing_users() const {
    static const sstring default_user_query = sprint(
            "SELECT * FROM %s.%s WHERE %s = ?",
            meta::AUTH_KS,
            CREDENTIALS_CF,
            USER_NAME);

    static const sstring all_users_query = sprint(
            "SELECT * FROM %s.%s LIMIT 1",
            meta::AUTH_KS,
            CREDENTIALS_CF);

    // This logic is borrowed directly from Apache Cassandra. By first checking for the presence of the default user, we
    // can potentially avoid doing a range query with a high consistency level.

    return _qp.process(
            default_user_query,
            db::consistency_level::ONE,
            { meta::DEFAULT_SUPERUSER_NAME },
            true).then([this](auto results) {
        if (!results->empty()) {
            return make_ready_future<bool>(true);
        }

        return _qp.process(
                default_user_query,
                db::consistency_level::QUORUM,
                { meta::DEFAULT_SUPERUSER_NAME },
                true).then([this](auto results) {
            if (!results->empty()) {
                return make_ready_future<bool>(true);
            }

            return _qp.process(
                    all_users_query,
                    db::consistency_level::QUORUM).then([](auto results) {
                return make_ready_future<bool>(!results->empty());
            });
        });
    });
}
