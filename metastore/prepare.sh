#!/bin/bash

HADOOP_VERSION="3.2.1"
HIVE_METASTORE_VERSION="3.1.2"
POSTGRES_JDBC_VERSION="42.2.16"
HADOOP_DIR="hadoop-${HADOOP_VERSION}"
HIVE_METASTORE_DIR="metastore"

# Download Hadoop
if [ -d $HADOOP_DIR ]
then
    echo "Hadoop already downloaded"
else
    HADOOP_DOWNLOAD_URL="http://archive.apache.org/dist/hadoop/common/hadoop-$HADOOP_VERSION/hadoop-$HADOOP_VERSION.tar.gz"
    echo "Downloading Hadoop $HADOOP_VERSION from $HADOOP_DOWNLOAD_URL"
    curl -O $HADOOP_DOWNLOAD_URL
    tar -xvf "hadoop-$HADOOP_VERSION.tar.gz"
    rm -f hadoop-$HADOOP_VERSION.tar.gz
    echo "Completed downloading Hadoop $HADOOP_VERSION"
fi

# Download Hive Metastore Standalone
if [ -d $HIVE_METASTORE_DIR ]
then
  echo "Hive Metastore already downloaded"
else
  echo "Downloading Hive Metastore $HIVE_METASTORE_VERSION"
  curl -O https://repo1.maven.org/maven2/org/apache/hive/hive-standalone-metastore/$HIVE_METASTORE_VERSION/hive-standalone-metastore-$HIVE_METASTORE_VERSION-bin.tar.gz
  echo "Unzipping $HIVE_METASTORE_VERSION"
  tar -xvf hive-standalone-metastore-$HIVE_METASTORE_VERSION-bin.tar.gz
  rm -f hive-standalone-metastore-$HIVE_METASTORE_VERSION-bin.tar.gz
  mv apache-hive-metastore-$HIVE_METASTORE_VERSION-bin metastore
  echo "Completed downloading Hive Metastore $HIVE_METASTORE_VERSION"
fi

# Download Postgres JDBC Driver
echo "Downloading Postgres JDBC Driver $POSTGRES_JDBC_VERSION"
curl -O https://jdbc.postgresql.org/download/postgresql-$POSTGRES_JDBC_VERSION.jar
echo "Completed downloading Postgres JDBC Driver $POSTGRES_JDBC_VERSION"


