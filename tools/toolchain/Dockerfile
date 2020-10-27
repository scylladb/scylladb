FROM docker.io/fedora:33
ADD ./install-dependencies.sh ./
ADD ./seastar/install-dependencies.sh ./seastar/
ADD ./tools/toolchain/system-auth ./
RUN dnf -y install 'dnf-command(copr)' \
    && dnf -y install ccache \
    && dnf -y install gdb \
    && dnf -y install devscripts debhelper fakeroot file rpm-build \
    && /bin/bash -x ./install-dependencies.sh && dnf -y update && dnf clean all \
    && echo 'ALL ALL=(ALL:ALL) NOPASSWD: ALL' >> /etc/sudoers \
    && cp system-auth /etc/pam.d \
    && echo 'Defaults !requiretty' >> /etc/sudoers
RUN mkdir -p /root/.m2/repository
ENV JAVA8_HOME=/usr/lib/jvm/java-1.8.0-openjdk
CMD /bin/bash
