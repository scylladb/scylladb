/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 */

#ifndef VIRTIO_HH_
#define VIRTIO_HH_

#include <memory>
#include "net.hh"
#include "sstring.hh"

std::unique_ptr<net::device> create_virtio_net_device(sstring tap_device);

#endif /* VIRTIO_HH_ */
