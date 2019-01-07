FROM fedora:29
ADD ./install-dependencies.sh ./
ADD ./seastar/install-dependencies.sh ./seastar/
ADD ./tools/toolchain/system-auth ./
RUN dnf -y install 'dnf-command(copr)' \
    && dnf -y copr enable scylladb/toolchain \
    && dnf -y install ccache \
    && /bin/bash -x ./install-dependencies.sh && dnf -y update \
    && sed -i 's/^\(%wheel.*\)ALL/\1NOPASSWD: ALL/' /etc/sudoers \
    && cp system-auth /etc/pam.d \
    && echo 'Defaults !requiretty' >> /etc/sudoers

