FROM docker.io/fedora:34 AS base
ADD ./install-dependencies.sh ./
ADD ./seastar/install-dependencies.sh ./seastar/
ADD ./tools/jmx/install-dependencies.sh ./tools/jmx/
ADD ./tools/java/install-dependencies.sh ./tools/java/
ADD ./tools/toolchain/system-auth ./
RUN dnf -y update \
    && dnf -y install 'dnf-command(copr)' \
    && dnf -y install ccache \
    && dnf -y install devscripts debhelper fakeroot file rpm-build \
    && ./install-dependencies.sh && dnf clean all \
    && echo 'ALL ALL=(ALL:ALL) NOPASSWD: ALL' >> /etc/sudoers \
    && cp system-auth /etc/pam.d \
    && echo 'Defaults !requiretty' >> /etc/sudoers
RUN dnf -y install llvm
RUN mkdir -p /root/.m2/repository

FROM base AS bolt
RUN git clone https://github.com/llvm/llvm-project --branch llvmorg-14.0.3 --depth 1
ADD ./tools/toolchain/bolt-fix.patch ./
RUN env GIT_COMMITTER_NAME='ScyllaDB' GIT_COMMITTER_EMAIL='scylla@example.com' git -C llvm-project am ../bolt-fix.patch
WORKDIR llvm-project
RUN cmake -B build -S llvm -DLLVM_ENABLE_PROJECTS='bolt' -G Ninja -DLLVM_TARGETS_TO_BUILD=X86 -DCMAKE_BUILD_TYPE=Release -DCMAKE_C_COMPILER=/usr/bin/clang -DCMAKE_CXX_COMPILER=/usr/bin/clang++ -DLLVM_CCACHE_BUILD=yes -DLLVM_ENABLE_LLD=yes -DLLVM_BUILD_RUNTIME=No
RUN cmake --build build --target install

FROM base
COPY --from=bolt /usr/local/bin/llvm-bolt /usr/local/bin/merge-fdata /usr/local/bin
COPY --from=bolt /usr/local/lib/libbolt_rt_instr.a /usr/local/lib
ENV JAVA8_HOME=/usr/lib/jvm/java-1.8.0-openjdk
CMD /bin/bash
