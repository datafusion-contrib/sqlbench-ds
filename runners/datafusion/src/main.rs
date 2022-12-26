use datafusion::common::{DataFusionError, Result};
use datafusion::datasource::MemTable;
use datafusion::prelude::{ParquetReadOptions, SessionConfig, SessionContext};
use qpml::from_datafusion;
use std::fs;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use structopt::StructOpt;
use tokio::time::Instant;

const TABLES: &[&str] = &[
    "call_center",
    "customer_address",
    "household_demographics",
    "promotion",
    "store_returns",
    "web_page",
    "catalog_page",
    "customer_demographics",
    "income_band",
    "reason",
    "store_sales",
    "web_returns",
    "catalog_returns",
    "customer",
    "inventory",
    "ship_mode",
    "time_dim",
    "web_sales",
    "catalog_sales",
    "date_dim",
    "item",
    "store",
    "warehouse",
    "web_site",
];

/// A basic example
#[derive(StructOpt, Debug)]
#[structopt(name = "basic")]
struct Opt {
    /// Activate debug mode
    #[structopt(long)]
    debug: bool,

    /// Path to TPC-DS queries
    #[structopt(long, parse(from_os_str))]
    query_path: PathBuf,

    /// Path to TPC-DS data set
    #[structopt(short, long, parse(from_os_str))]
    data_path: PathBuf,

    /// Query number. If no query number specified then all queries will be executed.
    #[structopt(short, long)]
    query: Option<u8>,

    /// Concurrency
    #[structopt(short, long)]
    concurrency: u8,
}

#[tokio::main]
pub async fn main() -> Result<()> {
    let opt = Opt::from_args();

    let query_path = format!("{}", opt.query_path.display());
    let data_path = format!("{}", opt.data_path.display());

    match opt.query {
        Some(query) => {
            execute_query(&query_path, query, &data_path, opt.concurrency, opt.debug).await?;
        }
        _ => {
            for query in 1..=99 {
                println!("Executing query {}", query);
                let result =
                    execute_query(&query_path, query, &data_path, opt.concurrency, opt.debug).await;
                match result {
                    Ok(_) => {}
                    Err(e) => println!("Fail: {}", e),
                }
            }
        }
    }

    Ok(())
}

pub async fn execute_query(
    query_path: &str,
    query_no: u8,
    data_path: &str,
    target_partitions: u8,
    debug: bool,
) -> Result<()> {
    let filename = format!("{}/{query_no}.sql", query_path);
    let sql = fs::read_to_string(filename).expect("Could not read query sql");

    let config = SessionConfig::from_env().with_target_partitions(target_partitions as usize);

    let ctx = SessionContext::with_config(config);

    for table in TABLES {
        // let path = format!("{}/{}", &data_path, table.name);
        let path = format!("{}/{}.parquet", &data_path, table);

        if Path::new(&path).exists() {
            ctx.register_parquet(table, &path, ParquetReadOptions::default())
                .await?;
        } else {
            return Err(DataFusionError::Execution(format!(
                "Path does not exist: {}",
                path
            )));
        }
    }

    // some queries have multiple statements
    let sql = sql
        .split(';')
        .filter(|s| !s.trim().is_empty())
        .collect::<Vec<_>>();

    let multipart = sql.len() > 1;

    for (i, sql) in sql.iter().enumerate() {
        if debug {
            println!("Query {}: {}", query_no, sql);
        }

        let file_suffix = if multipart {
            format!("_part{}", i + 1)
        } else {
            "".to_owned()
        };

        let df = ctx.sql(sql).await?;
        let plan = df.to_logical_plan()?;
        let formatted_query_plan = format!("{}", plan.display_indent());
        let filename = format!("q{}{}-logical-plan.txt", query_no, file_suffix);
        let mut file = File::create(&filename)?;
        write!(file, "{}", formatted_query_plan)?;

        // write QPML
        let qpml = from_datafusion(&plan);
        let filename = format!("q{}{}.qpml", query_no, file_suffix);
        let file = File::create(&filename)?;
        let mut file = BufWriter::new(file);
        serde_yaml::to_writer(&mut file, &qpml).unwrap();

        let start = Instant::now();
        let batches = df.collect().await?;
        let duration = start.elapsed();
        println!("Query {} executed in: {:?}", query_no, duration);

        // write results to disk
        if !batches.is_empty() {
            let filename = format!("q{}{}.csv", query_no, file_suffix);
            let t = MemTable::try_new(batches[0].schema(), vec![batches])?;
            let df = ctx.read_table(Arc::new(t))?;
            df.write_csv(&filename).await?;
        }
    }

    Ok(())
}
