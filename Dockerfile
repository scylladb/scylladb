FROM fedora

RUN yum install -y fedora-release-rawhide
RUN yum --enablerepo rawhide install -y gcc-c++ libasan libubsan
RUN yum install -y make python3 gperftools-devel libaio-devel ninja-build boost-devel git
