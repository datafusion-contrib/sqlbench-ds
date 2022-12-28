# Spark SQLBench-DS Benchmarks

## Pre-requisites

- Download Apache Maven from https://maven.apache.org/download.cgi
- Download Apache Spark 3.3.0 from https://spark.apache.org/downloads.html

Untar these downloads and set `MAVEN_HOME` and `SPARK_HOME` environment variables to point to the 
install location.

## Build the benchmark JAR file

```bash
$MAVEN_HOME/bin/mvn package
```

# Standalone Mode

## Start a local Spark cluster in standalone mode

```bash
$SPARK_HOME/sbin/start-master.sh
$SPARK_HOME/sbin/start-worker.sh spark://ripper:7077
```

## Run Single Query

```bash
$SPARK_HOME/bin/spark-submit --master spark://ripper:7077 \
    --class io.sqlbenchmarks.sqlbenchds.Main \
    --conf spark.driver.memory=8G \
    --conf spark.executor.memory=32G \
    --conf spark.executor.cores=24 \
    target/sqlbench-ds-0.1.0-SNAPSHOT-jar-with-dependencies.jar \
    --input-path /mnt/bigdata/tpcds/sf100-parquet/ \
    --query-path ~/git/sql-benchmarks/sqlbench-ds-private/queries-spark/sf\=100/ \
    --query 1
```

## Run All Queries

```bash
$SPARK_HOME/bin/spark-submit --master spark://ripper:7077 \
    --class io.sqlbenchmarks.sqlbenchds.Main \
    --conf spark.driver.memory=8G \
    --conf spark.executor.memory=32G \
    --conf spark.executor.cores=24 \
    target/sqlbench-ds-0.1.0-SNAPSHOT-jar-with-dependencies.jar \
    --input-path /mnt/bigdata/tpcds/sf100-parquet/ \
    --query-path ~/git/sql-benchmarks/sqlbench-ds-private/queries-spark/sf\=100/ \
    --skip-query72
```

Monitor progress via the Spark UI at http://localhost:8080

## Shut down the cluster

```bash
$SPARK_HOME/sbin/stop-slave.sh
$SPARK_HOME/sbin/stop-master.sh
```