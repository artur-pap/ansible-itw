# Version 0.1
FROM java:latest
MAINTAINER EPAM
ENV REFRESHEDAT 2017-03-28

ENV DEBIANFRONTEND noninteractive

RUN apt-get update
RUN apt-get dist-upgrade -y
RUN apt-get -y install lsb-release

# Setup
RUN apt-key adv --keyserver keyserver.ubuntu.com --recv E56151BF
RUN export OSDISTRO=$(lsb_release -is | tr '[:upper:]' '[:lower:]') && \
export OSCODENAME=$(lsb_release -cs) && \
echo "deb http://repos.mesosphere.io/${OSDISTRO} ${OSCODENAME} main" | \
tee /etc/apt/sources.list.d/mesosphere.list &&\
apt-get -y update

RUN apt-get -y install mesos

#RUN apt-get install -y python libnss3 curl
#RUN apt-get install -y python-pip python-dev build-essential

RUN curl http://d3kbcqa49mib13.cloudfront.net/spark-2.0.1-bin-hadoop2.7.tgz \
| tar -xzC /opt && \
mv /opt/spark* /spark

RUN apt-get clean

# Fix pypspark six error.
#RUN pip2 install -U six
#RUN pip2 install msgpack-python
#RUN pip2 install avro

COPY docker/spark-conf/* /spark/conf/
COPY docker/scripts /scripts
#COPY target/scala-2.11/spark-reports-assembly-0.7-SNAPSHOT.jar /scripts/spark-reports-assembly-0.7-SNAPSHOT.jar

ENV SPARKHOME /spark
WORKDIR /spark

ENTRYPOINT ["/scripts/run.sh"]