FROM ubuntu:16.04
RUN apt-get update
RUN apt-get install -y wget vim
RUN wget -O /etc/apt/sources.list.d/scylla.list http://downloads.scylladb.com/deb/ubuntu/scylla-1.3-xenial.list
RUN apt-get update && apt-get install -y scylla-server scylla-jmx scylla-tools --force-yes

USER root
COPY start-scylla /start-scylla
RUN  chown -R scylla:scylla /etc/scylla
RUN  chown -R scylla:scylla /etc/scylla.d
RUN  chown -R scylla:scylla /start-scylla

USER scylla
EXPOSE 10000 9042 9160 7000 7001
VOLUME /var/lib/scylla

CMD /start-scylla
