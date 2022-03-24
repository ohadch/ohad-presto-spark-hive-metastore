#!/bin/sh

export HADOOP_HOME=/opt/hadoop-3.2.1
export HADOOP_CLASSPATH=${HADOOP_HOME}/share/hadoop/tools/lib/aws-java-sdk-bundle-1.11.375.jar:${HADOOP_HOME}/share/hadoop/tools/lib/hadoop-aws-3.2.1.jar
export JAVA_HOME=/usr/local/openjdk-8

/opt/apache-hive-metastore-3.1.2-bin/bin/schematool -initSchema -dbType postgres -verbose
/opt/apache-hive-metastore-3.1.2-bin/bin/start-metastore
