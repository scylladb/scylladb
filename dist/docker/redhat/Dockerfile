FROM centos:7

MAINTAINER Avi Kivity <avi@cloudius-systems.com>

ENV container docker

# The SCYLLA_REPO_URL argument specifies the URL to the RPM repository this Docker image uses to install Scylla. The default value is the Scylla's unstable RPM repository, which contains the daily build.
ARG SCYLLA_REPO_URL=http://downloads.scylladb.com/rpm/unstable/centos/scylla-4.3/latest/scylla.repo
ARG VERSION=4.3.rc0

ADD scylla_bashrc /scylla_bashrc

# Scylla configuration:
ADD etc/sysconfig/scylla-server /etc/sysconfig/scylla-server

# Supervisord configuration:
ADD etc/supervisord.conf /etc/supervisord.conf
ADD etc/supervisord.conf.d/scylla-server.conf /etc/supervisord.conf.d/scylla-server.conf
ADD etc/supervisord.conf.d/scylla-housekeeping.conf /etc/supervisord.conf.d/scylla-housekeeping.conf
ADD etc/supervisord.conf.d/sshd-server.conf /etc/supervisord.conf.d/sshd-server.conf
ADD etc/supervisord.conf.d/scylla-jmx.conf /etc/supervisord.conf.d/scylla-jmx.conf
ADD etc/supervisord.conf.d/node-exporter.conf /etc/supervisord.conf.d/node-exporter.conf
ADD etc/supervisord.conf.d/rsyslog.conf /etc/supervisord.conf.d/rsyslog.conf
ADD etc/rsyslog.conf /etc/rsyslog.conf
ADD scylla-service.sh /scylla-service.sh
ADD node-exporter-service.sh /node-exporter-service.sh
ADD scylla-housekeeping-service.sh /scylla-housekeeping-service.sh
ADD scylla-jmx-service.sh /scylla-jmx-service.sh
ADD sshd-service.sh /sshd-service.sh

# Docker image startup scripts:
ADD scyllasetup.py /scyllasetup.py
ADD commandlineparser.py /commandlineparser.py
ADD docker-entrypoint.py /docker-entrypoint.py
ADD node_exporter_install /node_exporter_install
# Install Scylla:
RUN curl $SCYLLA_REPO_URL -o /etc/yum.repos.d/scylla.repo && \
    yum -y install epel-release && \
    yum -y clean expire-cache && \
    yum -y update && \
    yum -y install scylla-$VERSION hostname supervisor openssh-server openssh-clients rsyslog && \
    yum clean all && \
    cat /scylla_bashrc >> /etc/bashrc && \
    mkdir -p /etc/supervisor.conf.d && \
    mkdir -p /var/log/scylla && \
    /node_exporter_install && \
    chown -R scylla.scylla /var/lib/scylla

ENV PATH /opt/scylladb/python3/bin:$PATH
ENTRYPOINT ["/docker-entrypoint.py"]

EXPOSE 10000 9042 9160 9180 7000 7001 22
VOLUME [ "/var/lib/scylla" ]
