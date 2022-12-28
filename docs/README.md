# SQLBench-DS Documentation

## Download TPC-DS Tools

Download TPC-DS Tools from [this link](https://www.tpc.org/tpc_documents_current_versions/download_programs/tools-download-request5.asp?bm_type=TPC-DS&bm_vers=3.2.0&mode=CURRENT-ONLY).

Unzip the file and build the binaries.

```bash
cd DSGen-software-code-3.2.0rc1
cd tools
make
```

## Generating a Query Stream

Create a `postgres.tpl` file in the `query_templates` directory.

```
define __LIMITA = "";
define __LIMITB = "";
define __LIMITC = " LIMIT %d";
define _BEGIN = "-- start query " + [_QUERY] + " in stream " + [_STREAM] + " using template " + [_TEMPLATE];
define _END = "-- end query " + [_QUERY] + " in stream " + [_STREAM] + " using template " + [_TEMPLATE];
```

Run qsgen with the desired scale factor.

```bash
./dsqgen -scale 100 -directory ../query_templates -input ../query_templates/templates.lst -dialect postgres -output_dir /tmp
```

The generated query stream will contain comments such as:

```
-- start query 1 in stream 0 using template query96.tpl
```

We need to process this file and split the queries out into individual files.



## Spark

- Double quotes need to be translated into backticks
- `` `90 days` `` needs to be translated to `INTERVAL '90 days'`