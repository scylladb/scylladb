/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Copyright 2015 Cloudius Systems
 *
 * Modified by Cloudius Systems
 */

#pragma once

#include "selectable.hh"
#include "cql3/column_identifier.hh"

namespace cql3 {

namespace selection {

class selectable::writetime_or_ttl : public selectable {
public:
    shared_ptr<column_identifier> _id;
    bool _is_writetime;

    writetime_or_ttl(shared_ptr<column_identifier> id, bool is_writetime)
            : _id(std::move(id)), _is_writetime(is_writetime) {
    }

#if 0
    @Override
    public String toString()
    {
        return (isWritetime ? "writetime" : "ttl") + "(" + id + ")";
    }
#endif

    virtual shared_ptr<selector::factory> new_selector_factory(schema_ptr s, std::vector<const column_definition*>& defs) override;

    class raw : public selectable::raw {
        shared_ptr<column_identifier::raw> _id;
        bool _is_writetime;
    public:
        raw(shared_ptr<column_identifier::raw> id, bool is_writetime)
            : _id(std::move(id)), _is_writetime(is_writetime) {
        }
        virtual shared_ptr<selectable> prepare(schema_ptr s) override;
        virtual bool processes_selection() const override;
    };
};

}

}
