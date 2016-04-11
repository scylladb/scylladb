FROM centos:7

MAINTAINER Avi Kivity <avi@cloudius-systems.com>

RUN yum -y install epel-release
ADD scylla.repo /etc/yum.repos.d/
RUN yum -y clean expire-cache
RUN yum -y update
RUN yum -y remove boost-thread boost-system
RUN yum -y install scylla-server scylla-jmx scylla-tools hostname
RUN yum clean all

ADD start-scylla /start-scylla
RUN chown scylla /start-scylla

ADD bashrc /var/lib/scylla/.bashrc
RUN chown scylla /var/lib/scylla/.bashrc

USER scylla
EXPOSE 10000 9042 9160 7000 7001
VOLUME /var/lib/scylla

CMD /start-scylla && /bin/bash
