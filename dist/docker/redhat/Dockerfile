FROM centos:7

MAINTAINER Avi Kivity <avi@cloudius-systems.com>

#enable systemd
ENV container docker
VOLUME [ "/sys/fs/cgroup" ]

#install scylla
RUN curl http://downloads.scylladb.com/rpm/centos/scylla-1.3.repo -o /etc/yum.repos.d/scylla.repo
RUN yum -y install epel-release
RUN yum -y clean expire-cache
RUN yum -y update
RUN yum -y remove boost-thread boost-system
RUN yum -y install scylla hostname supervisor
RUN yum clean all

#install python3 for our main script
RUN yum -y install python34 python34-PyYAML

ADD scylla_bashrc /scylla_bashrc
RUN cat /scylla_bashrc >> /etc/bashrc

# Scylla configuration:
ADD etc/sysconfig/scylla-server /etc/sysconfig/scylla-server

# Supervisord configuration:
ADD etc/supervisord.conf /etc/supervisord.conf
RUN mkdir -p /var/log/scylla
ADD scylla-service.sh /scylla-service.sh
ADD scylla-jmx-service.sh /scylla-jmx-service.sh

ADD scyllasetup.py /scyllasetup.py
ADD commandlineparser.py /commandlineparser.py
ADD docker-entrypoint.py /docker-entrypoint.py
ENTRYPOINT ["/docker-entrypoint.py"]

EXPOSE 10000 9042 9160 7000 7001
VOLUME [ "/var/lib/scylla" ]
RUN chown -R scylla.scylla /var/lib/scylla
