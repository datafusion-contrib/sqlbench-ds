# SQLBench-DS

## Overview

SQLBench-DS is a SQL benchmark derived from [TPC-DS](https://www.tpc.org/tpcds/) under the terms of the Transaction
Processing Council's [Fair Use Policy](https://www.tpc.org/tpc_documents_current_versions/pdf/tpc_fair_use_quick_reference_v1.0.0.pdf).

This benchmark is limited to measuring the execution time of individual queries derived from TPC-DS and is intended for
use as a way to compare performance between open source query engines.

## How does this differ from TPC-DS

- This benchmark only measures execution times for individual queries
- Only Parquet input files are supported

## Legal Stuff

SQLBench-DS is a Non-TPC Benchmark. Any comparison between official TPC Results with non-TPC workloads is prohibited by
the TPC.

TPC-DS is Copyright &copy; 2021 Transaction Processing Performance Council. The full TPC-DS specification in PDF
format can be found [here](https://www.tpc.org/tpc_documents_current_versions/pdf/tpc-ds_v3.2.0.pdf)

TPC, TPC Benchmark and TPC-DS are trademarks of the Transaction Processing Performance Council.