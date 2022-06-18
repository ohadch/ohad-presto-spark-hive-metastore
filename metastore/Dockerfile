FROM centos:7

ENV container docker

LABEL maintainer="Ohad Chaet"

# copy Certificate Authority file
COPY ca.pem /etc/pki/ca-trust/source/anchors/

# copy Hive standalone package
COPY metastore /opt/metastore/

# copy Hadoop package
COPY hadoop-3.2.1 /opt/hadoop-3.2.1/

# copy Postgres or MySQL JDBC connector
COPY postgresql-42.2.16.jar /opt/metastore/lib/

# add Certificate Authority to database
RUN update-ca-trust

WORKDIR /install

# install Java 1.8 and clean cache
RUN yum install -y java-1.8.0-openjdk-devel \
  && yum clean all

# environment variables requested by Hive metastore
ENV JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk
ENV HADOOP_HOME=/opt/hadoop-3.2.1

# replace a library and add missing libraries
RUN rm -f /opt/metastore/lib/guava-19.0.jar \
  && cp ${HADOOP_HOME}/share/hadoop/common/lib/guava-27.0-jre.jar /opt/metastore/lib \
  && cp ${HADOOP_HOME}/share/hadoop/tools/lib/hadoop-aws-3.2.1.jar /opt/metastore/lib \
  && cp ${HADOOP_HOME}/share/hadoop/tools/lib/aws-java-sdk-bundle-1.11.375.jar /opt/metastore/lib

WORKDIR /opt/metastore

# copy Hive metastore configuration file
COPY metastore-site.xml /opt/metastore/conf/

# Hive metastore data folder
VOLUME ["/user/hive/warehouse"]

COPY ./entrypoint.sh /opt/entrypoint.sh
RUN ["chmod", "+x", "/opt/entrypoint.sh"]


ENTRYPOINT ["/opt/entrypoint.sh"]