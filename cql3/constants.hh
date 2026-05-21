/*
 * Copyright (C) 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (LicenseRef-ScyllaDB-Source-Available-1.1 and Apache-2.0)
 */

#pragma once

#include "cql3/update_parameters.hh"
#include "cql3/operation.hh"
#include "cql3/values.hh"
#include "mutation/mutation.hh"
#include <seastar/core/shared_ptr.hh>

namespace service::broadcast_tables {
    class update_query;
}

namespace cql3 {

/**
 * Static helper methods and classes for constants.
 */
class constants {
public:
#if 0
    private static final Logger logger = LoggerFactory.getLogger(Constants.class);
#endif
public:
    class setter : public operation_skip_if_unset {
        bool _requires_read;
    public:
        setter(const column_definition& column, expr::expression e);

        virtual bool requires_read() const override { return _requires_read; }
        virtual bool requires_lwt() const override {
            // An expression that requires reading a value from the row, e.g.,
            // SET r = r + 1, requires LWT for atomicity and should not be allowed
            // on non-LWT requests. In one special case, namely a list update
            // "UPDATE t SET mylist[0] = 5 WHERE ...", we do allow reading the old
            // row without LWT or atomicity - but this case is not handled by
            // constants::setter so we don't need to support it here.
            return _requires_read;
        }
        virtual void execute(mutation& m, const clustering_key_prefix& prefix, const update_parameters& params) override;

        static void execute(mutation& m, const clustering_key_prefix& prefix, const update_parameters& params, const column_definition& column, cql3::raw_value_view value);

        virtual expr::expression prepare_new_value_for_broadcast_tables() const override;
        size_t object_size() const override { return sizeof(*this); }
    };

    struct adder final : operation_skip_if_unset {
        using operation_skip_if_unset::operation_skip_if_unset;

        virtual void execute(mutation& m, const clustering_key_prefix& prefix, const update_parameters& params) override;
    };

    struct subtracter final : operation_skip_if_unset {
        using operation_skip_if_unset::operation_skip_if_unset;

        virtual void execute(mutation& m, const clustering_key_prefix& prefix, const update_parameters& params) override;
    };

    class deleter : public operation_no_unset_support {
    public:
        deleter(const column_definition& column)
            : operation_no_unset_support(column, std::nullopt)
        { }

        virtual void execute(mutation& m, const clustering_key_prefix& prefix, const update_parameters& params) override;
    };
};

}
