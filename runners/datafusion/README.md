# SQLBench-DS: DataFusion Runner

```bash
./target/release/datafusion-sqlbench-ds \
  --concurrency 24 \
  --data-path /mnt/bigdata/tpcds/sf100-parquet/ \
  --query-path=/home/andy/git/sql-benchmarks/sqlbench-ds-private/queries/sf\=100/ \
  --output .
```