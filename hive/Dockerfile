FROM openjdk:8u242-jre

WORKDIR /opt

ENV HADOOP_VERSION=3.2.1
ENV METASTORE_VERSION=3.1.2
ENV HADOOP_HOME=/opt/hadoop-${HADOOP_VERSION}
ENV METASTORE_HOME=/opt/apache-hive-metastore-${METASTORE_VERSION}-bin

COPY third-party/hive-standalone-metastore-${METASTORE_VERSION}-bin.tar.gz /opt
COPY third-party/hadoop-${HADOOP_VERSION}.tar.gz /opt

RUN tar -xzf hive-standalone-metastore-${METASTORE_VERSION}-bin.tar.gz
RUN tar -xzf hadoop-${HADOOP_VERSION}.tar.gz

RUN rm -f ${METASTORE_HOME}/lib/guava-19.0.jar

#COPY third-party/aws-java-sdk-1.11.30.jar ${METASTORE_HOME}/lib
#COPY third-party/hadoop-aws-${HADOOP_VERSION}.jar ${METASTORE_HOME}/lib
COPY third-party/jets3t-0.9.4.jar ${METASTORE_HOME}/lib
COPY third-party/postgresql-42.3.3.jar ${METASTORE_HOME}/lib

# replace a library and add missing libraries
RUN rm -f ${METASTORE_HOME}/lib/guava-19.0.jar \
  && cp ${HADOOP_HOME}/share/hadoop/common/lib/guava-27.0-jre.jar ${METASTORE_HOME}/lib \
  && cp ${HADOOP_HOME}/share/hadoop/tools/lib/aws-java-sdk-bundle-1.11.375.jar ${METASTORE_HOME}/lib


COPY conf/metastore-site.xml ${METASTORE_HOME}/conf
COPY scripts/entrypoint.sh /entrypoint.sh

RUN groupadd -r hive --gid=1000 && \
    useradd -r -g hive --uid=1000 -d ${METASTORE_HOME} hive && \
    chown hive:hive -R ${METASTORE_HOME} && \
    chown hive:hive /entrypoint.sh && chmod +x /entrypoint.sh

RUN mkdir /opt/warehouse && chown hive:hive /opt/warehouse

USER hive
EXPOSE 9083

ENTRYPOINT ["sh", "-c", "/entrypoint.sh"]
