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

#ifndef VIRTIO_HH_
#define VIRTIO_HH_

#include <memory>
#include "net.hh"
#include "core/sstring.hh"

std::unique_ptr<net::device> create_virtio_net_device(boost::program_options::variables_map opts = boost::program_options::variables_map());
boost::program_options::options_description get_virtio_net_options_description();

#endif /* VIRTIO_HH_ */
