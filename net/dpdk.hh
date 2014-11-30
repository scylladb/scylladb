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
                                boost::program_options::variables_map opts =
                                     boost::program_options::variables_map(),
                                uint8_t num_queues = 1);

boost::program_options::options_description get_dpdk_net_options_description();

#endif // _SEASTAR_DPDK_DEV_H

#endif // HAVE_DPDK
