#!/bin/bash

HIVE_METASTORE_VERSION="3.1.2"
POSTGRES_JDBC_VERSION="42.2.16"

# Remove old metastore and driver
echo "Removing old metastore and JDBC driver"
rm -rf metastore
rm postgresql-$POSTGRES_JDBC_VERSION.jar
echo "Finished removing old metastore and driver"

# Download Hive Metastore Standalone
echo "Downloading Hive Metastore $HIVE_METASTORE_VERSION"
curl -O https://repo1.maven.org/maven2/org/apache/hive/hive-standalone-metastore/$HIVE_METASTORE_VERSION/hive-standalone-metastore-$HIVE_METASTORE_VERSION-bin.tar.gz

echo "Unzipping $HIVE_METASTORE_VERSION"
tar -xvf hive-standalone-metastore-$HIVE_METASTORE_VERSION-bin.tar.gz
rm -f hive-standalone-metastore-$HIVE_METASTORE_VERSION-bin.tar.gz
mv apache-hive-metastore-$HIVE_METASTORE_VERSION-bin metastore

echo "Completed downloading Hive Metastore $HIVE_METASTORE_VERSION"

# Download Postgres JDBC Driver
echo "Downloading Postgres JDBC Driver $POSTGRES_JDBC_VERSION"
curl -O https://jdbc.postgresql.org/download/postgresql-$POSTGRES_JDBC_VERSION.jar
echo "Completed downloading Postgres JDBC Driver $POSTGRES_JDBC_VERSION"


