FROM fedora:22

MAINTAINER Avi Kivity <avi@cloudius-systems.com>

ADD scylla.repo /etc/yum.repos.d/
RUN dnf -y update
RUN dnf -y install scylla-server hostname
RUN dnf clean all
ADD start-scylla /start-scylla
RUN chown scylla /start-scylla

USER scylla
EXPOSE 10000 9042 9160 7000 7001
VOLUME /var/lib/scylla

CMD /start-scylla
