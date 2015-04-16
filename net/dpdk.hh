/*
 * This file is open source software, licensed to you under the terms
 * of the Apache License, Version 2.0 (the "License").  See the NOTICE file
 * distributed with this work for additional information regarding copyright
 * ownership.  You may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at
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
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 */

#ifdef HAVE_DPDK

#ifndef _SEASTAR_DPDK_DEV_H
#define _SEASTAR_DPDK_DEV_H

#include <memory>
#include "net.hh"
#include "core/sstring.hh"

std::unique_ptr<net::device> create_dpdk_net_device(
                                    uint8_t port_idx = 0,
                                    uint8_t num_queues = 1,
                                    bool use_lro = true,
                                    bool enable_fc = true);

boost::program_options::options_description get_dpdk_net_options_description();

namespace dpdk {
/**
 * @return Number of bytes needed for mempool objects of each QP.
 */
uint32_t qp_mempool_obj_size(bool hugetlbfs_membackend);
}

#endif // _SEASTAR_DPDK_DEV_H

#endif // HAVE_DPDK
