/*
 * Copyright (C) 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

#include "cql3/expr/expression.hh"
#include "cql3/column_identifier.hh"
#include "data_dictionary/data_dictionary.hh"

namespace cql3 {

namespace selection {

struct prepared_selector;

class raw_selector {
public:
    const expr::expression selectable_;
    const ::shared_ptr<column_identifier> alias;

    raw_selector(expr::expression selectable__, shared_ptr<column_identifier> alias_)
        : selectable_{std::move(selectable__)}
        , alias{alias_}
    { }

    /**
     * Converts the specified list of <code>RawSelector</code>s into a list of <code>Selectable</code>s.
     *
     * @param raws the <code>RawSelector</code>s to converts.
     * @return a list of <code>Selectable</code>s
     */
    static std::vector<prepared_selector> to_prepared_selectors(const std::vector<::shared_ptr<raw_selector>>& raws,
            const schema& schema, data_dictionary::database db, const sstring& ks);
};

}

}
