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
                                    uint8_t num_queues = 1);

boost::program_options::options_description get_dpdk_net_options_description();

namespace dpdk {
/**
 * @return Number of bytes needed for mempool objects of each QP.
 */
uint32_t qp_mempool_obj_size();
}

#endif // _SEASTAR_DPDK_DEV_H

#endif // HAVE_DPDK
