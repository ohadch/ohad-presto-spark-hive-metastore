# Presto/Spark with Hive Metastore Showcase

Showcase for running Presto and Spark with Hive Metastore using PySpark in order to query S3 data. 

## Setup

### Download required jars to `./hive/third-party/`:
- hadoop-3.2.1.tar.gz
- hive-metastore-3.1.2.jar
- jets3t-0.9.4.jar
- hadoop-aws-3.2.1.jar
- hive-standalone-metastore-3.1.2-bin.tar.gz
- postgresql-42.3.3.jar
- Run `docker-compose up -d`

### Add secret keys:
- `hive/conf/metastore-site.xml`
- `presto/coordinator/etc/catalog/hive.properties`

### Create a virtual environment:
```
virtualenv venv
source venv/bin/activate
pip install -r requirements.txt
```

## Run the Showcase
```bash
docker-compose up -d
```