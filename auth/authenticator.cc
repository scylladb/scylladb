/*
 * Copyright (C) 2016-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#include "auth/authenticator.hh"

#include "auth/authenticated_user.hh"
#include "auth/common.hh"
#include "auth/password_authenticator.hh"
#include "cql3/query_processor.hh"
#include "utils/class_registrator.hh"

const sstring auth::authenticator::USERNAME_KEY("username");
const sstring auth::authenticator::PASSWORD_KEY("password");
