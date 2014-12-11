/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 */

#ifndef VIRTIO_HH_
#define VIRTIO_HH_

#include <memory>
#include "net.hh"
#include "core/sstring.hh"

std::unique_ptr<net::device> create_virtio_net_device(boost::program_options::variables_map opts = boost::program_options::variables_map());
boost::program_options::options_description get_virtio_net_options_description();

#endif /* VIRTIO_HH_ */
