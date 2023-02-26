FROM docker.io/fedora:38

# CLANG_BUILD argument can be used to compile clang itself
# in an optimized manner, based on LTO, PGO and BOLT optimizations.
# Default is to skip this stage. Two other options are:
# BUILD - just build clang
# INSTALL - build *and* install it (to /usr/bin)
# To change the argument, use --build-arg CLANG_BUILD=BUILD|INSTALL
ARG CLANG_BUILD=""
ARG TARGETARCH

ADD ./install-dependencies.sh ./
ADD ./seastar/install-dependencies.sh ./seastar/
ADD ./tools/jmx/install-dependencies.sh ./tools/jmx/
ADD ./tools/java/install-dependencies.sh ./tools/java/
ADD ./tools/toolchain/clang_lto_pgo.sh ./
ADD ./tools/toolchain/system-auth ./
RUN dnf -y update \
    && dnf -y install 'dnf-command(copr)' \
    && dnf -y install ccache \
    && dnf -y install devscripts debhelper fakeroot file rpm-build \
    && ./install-dependencies.sh && dnf clean all \
    && echo 'ALL ALL=(ALL:ALL) NOPASSWD: ALL' >> /etc/sudoers \
    && cp system-auth /etc/pam.d \
    && echo 'Defaults !requiretty' >> /etc/sudoers
RUN mkdir -p /root/.m2/repository
ENV JAVA8_HOME=/usr/lib/jvm/java-1.8.0-openjdk
ENV CLANG_ARCH=$TARGETARCH
RUN if [ "$CLANG_ARCH" = "amd64" ] ; then (chmod +x ./clang_lto_pgo.sh && ./clang_lto_pgo.sh) else echo "Unsupported Arch ($CLANG_ARCH) for clang optimization" ; fi
CMD /bin/bash
