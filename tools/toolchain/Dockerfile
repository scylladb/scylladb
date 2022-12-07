FROM registry.fedoraproject.org/fedora-minimal:38
ADD ./install-dependencies.sh ./
ADD ./seastar/install-dependencies.sh ./seastar/
ADD ./tools/jmx/install-dependencies.sh ./tools/jmx/
ADD ./tools/java/install-dependencies.sh ./tools/java/
ADD ./tools/toolchain/system-auth ./

# hack so other files using dnf will still work with microdnf
RUN ln -s /usr/bin/microdnf /usr/bin/dnf

RUN dnf -y update \
    && dnf -y install 'dnf-command(copr)' \
    && dnf -y install ccache devscripts debhelper fakeroot file rpm-build \
    && ./install-dependencies.sh && dnf clean all \
    && echo 'ALL ALL=(ALL:ALL) NOPASSWD: ALL' >> /etc/sudoers \
    && cp system-auth /etc/pam.d \
    && echo 'Defaults !requiretty' >> /etc/sudoers
RUN mkdir -p /root/.m2/repository
ENV JAVA8_HOME=/usr/lib/jvm/java-1.8.0-openjdk
CMD /bin/bash
