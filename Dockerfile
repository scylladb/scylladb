FROM fedora:21

RUN yum install -y gcc-c++ libasan libubsan hwloc hcloc-devel
RUN yum install -y python3 libaio-devel ninja-build boost-devel git ragel
