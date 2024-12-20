/*
 * Copyright (C) 2019-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (LicenseRef-ScyllaDB-Source-Available-1.0 and Apache-2.0)
 */

#pragma once

#include <functional>
#include <optional>
#include <string_view>

#include <seastar/core/future.hh>
#include <seastar/core/sstring.hh>

#include "auth/authenticated_user.hh"
#include "bytes_fwd.hh"
#include "seastarx.hh"

namespace auth {

///
/// A stateful SASL challenge which supports many authentication schemes (depending on the implementation).
///
class sasl_challenge {
public:
    virtual ~sasl_challenge() = default;

    virtual bytes evaluate_response(bytes_view client_response) = 0;

    virtual bool is_complete() const = 0;

    virtual future<authenticated_user> get_authenticated_user() const = 0;

    virtual const sstring& get_username() const = 0;
};

class plain_sasl_challenge : public sasl_challenge {
public:
    using completion_callback = std::function<future<authenticated_user>(std::string_view, std::string_view)>;

    explicit plain_sasl_challenge(completion_callback f) : _when_complete(std::move(f)) {
    }

    virtual bytes evaluate_response(bytes_view) override;

    virtual bool is_complete() const override;

    virtual future<authenticated_user> get_authenticated_user() const override;

    virtual const sstring& get_username() const override;

private:
    std::optional<sstring> _username, _password;
    completion_callback _when_complete;
};

}
