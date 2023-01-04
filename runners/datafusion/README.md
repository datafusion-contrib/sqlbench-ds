# SQLBench-DS: DataFusion Runner

## Run Single Query

```bash
./target/release/datafusion-sqlbench-ds \
  --concurrency 24 \
  --data-path /mnt/bigdata/tpcds/sf100-parquet/ \
  --query-path=/home/andy/git/sql-benchmarks/sqlbench-ds-private/queries/sf\=100/ \
  --output /home/andy/git/sql-benchmarks/sqlbench-ds-private/results/datafusion/sf\=100/output \
  --query 1
```

## Run All Queries

```bash
./target/release/datafusion-sqlbench-ds \
  --concurrency 24 \
  --data-path /mnt/bigdata/tpcds/sf100-parquet/ \
  --query-path=/home/andy/git/sql-benchmarks/sqlbench-ds-private/queries/sf\=100/ \
  --output /home/andy/git/sql-benchmarks/sqlbench-ds-private/results/datafusion/sf\=100/output
```