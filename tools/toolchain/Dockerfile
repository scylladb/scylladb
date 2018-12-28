FROM fedora:29
ADD ./install-dependencies.sh ./
ADD ./seastar/install-dependencies.sh ./seastar/
RUN dnf -y install 'dnf-command(copr)' \
    && dnf -y copr enable scylladb/toolchain \
    && /bin/bash -x ./install-dependencies.sh && dnf -y update
