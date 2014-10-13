FROM fedora:21

RUN yum install -y gcc-c++ clang libasan libubsan hwloc hwloc-devel numactl-devel
RUN yum install -y python3 libaio-devel ninja-build boost-devel git ragel
