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
 * Copyright 2015 Cloudius Systems
 *
 * Modified by Cloudius Systems
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

#include "bytes.hh"
#include "keys.hh"
#include "utils/data_input.hh"
#include "utils/data_output.hh"
#include "db/serializer.hh"
#include "paging_state.hh"

service::pager::paging_state::paging_state(partition_key pk, std::experimental::optional<clustering_key> ck,
        uint32_t rem)
        : _partition_key(std::move(pk)), _clustering_key(std::move(ck)), _remaining(rem) {
}

::shared_ptr<service::pager::paging_state> service::pager::paging_state::deserialize(
        bytes_opt data) {
    if (!data) {
        return nullptr;
    }

    data_input in(*data);

    try {
        auto pk = db::serializer<partition_key_view>::read(in);
        auto ckv = db::serializer<std::experimental::optional<clustering_key_view>>::read(in);
        auto rem(in.read<uint32_t>());

        std::experimental::optional<clustering_key> ck;
        if (ckv) {
            ck = clustering_key(*ckv);
        }
        return ::make_shared<paging_state>(std::move(pk), std::move(ck), rem);
    } catch (...) {
        std::throw_with_nested(
                exceptions::protocol_exception(
                        "Invalid value for the paging state"));
    }
}

bytes_opt service::pager::paging_state::serialize() const {
    auto pkv = _partition_key.view();
    auto ckv = _clustering_key ?
                    std::experimental::optional<clustering_key_view>(
                                    _clustering_key->view()) :
                    std::experimental::optional<clustering_key_view> { };

    db::serializer<partition_key_view> pks(pkv);
    db::serializer<std::experimental::optional<clustering_key_view>> cks(ckv);

    auto size = pks.size() + cks.size() + sizeof(uint32_t);

    bytes res{bytes::initialized_later(), size};
    data_output out(res);

    pks.write(out);
    cks.write(out);
    out.write(_remaining);

    return {std::move(res)};
}
