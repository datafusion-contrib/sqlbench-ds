# Spark results on Workstation

## Command 

```bash
$SPARK_HOME/bin/spark-submit --master spark://ripper:7077 \
--class io.sqlbenchmarks.sqlbenchds.Main \
--conf spark.driver.memory=8G \
--conf spark.executor.memory=32G \
--conf spark.executor.cores=24 \
target/sqlbench-ds-0.1.0-SNAPSHOT-jar-with-dependencies.jar \
--input-path /mnt/bigdata/tpcds/sf100-parquet/ \
--query-path /path/to/queries
```