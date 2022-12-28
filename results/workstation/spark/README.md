# Spark results on Workstation

## Spark Version

Apache Spark 3.2.1

## Command 

```bash
$SPARK_HOME/bin/spark-submit --master spark://ripper:7077 \
--class io.sqlbenchmarks.sqlbenchds.Main \
--conf spark.driver.memory=8G \
--conf spark.executor.memory=32G \
--conf spark.executor.cores=24 \
target/sqlbench-ds-0.1.0-SNAPSHOT-jar-with-dependencies.jar \
--input-path /mnt/bigdata/tpcds/sf100-parquet/ \
--query-path ~/git/sql-benchmarks/sqlbench-ds-private/queries-spark/sf\=100/
```