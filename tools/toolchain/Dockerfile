FROM docker.io/fedora:38
ADD ./install-dependencies.sh ./
ADD ./seastar/install-dependencies.sh ./seastar/
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
CMD /bin/bash
