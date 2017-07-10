#!/bin/bash
#
# This file is open source software, licensed to you under the terms
# of the Apache License, Version 2.0 (the "License").  See the NOTICE file
# distributed with this work for additional information regarding copyright
# ownership.  You may not use this file except in compliance with the License.
#
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

. /etc/os-release

bash seastar/install-dependencies.sh

if [ "$ID" = "ubuntu" ] || [ "$ID" = "debian" ]; then
    apt -y install libyaml-cpp-dev libjsoncpp-dev
    echo antlr3 and thrift still missing - waiting for ppa
elif [ "$ID" = "centos" ] || [ "$ID" = "fedora" ]; then
    yum install -y yaml-cpp-devel thrift-devel antlr3-tool antlr3-C++-devel jsoncpp-devel
fi
