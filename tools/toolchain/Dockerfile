FROM fedora:29
ADD ./install-dependencies.sh ./
ADD ./seastar/install-dependencies.sh ./seastar/
RUN /bin/bash -x ./install-dependencies.sh && dnf -y update
