package io.sqlbenchmarks.sqlbenchds

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.internal.SQLConf
import org.rogach.scallop.ScallopConf

import java.io.{BufferedWriter, File, FileWriter}
import scala.io.Source

class Conf(args: Array[String]) extends ScallopConf(args) {
  val inputPath = opt[String](required = true)
  val queryPath = opt[String](required = true)
  val query = opt[String](required = false)
  val keepAlive = opt[Boolean](required = false)
  val skipQuery72 = opt[Boolean](required = false)
  verify()
}

object Main {

  val tables = Seq(
    "call_center", "customer_address", "household_demographics", "promotion", "store_returns", "web_page",
    "catalog_page", "customer_demographics", "income_band", "reason", "store_sales", "web_returns",
    "catalog_returns", "customer", "inventory", "ship_mode", "time_dim", "web_sales", "catalog_sales",
    "date_dim", "item", "store", "warehouse", "web_site"
  )

  def main(args: Array[String]): Unit = {
    val conf = new Conf(args)

    val w = new BufferedWriter(new FileWriter(new File("results.csv")))

    val spark: SparkSession = SparkSession.builder
      .appName("SQLBench-DS Benchmarks")
      .config(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key, "-1")
      .getOrCreate()

    // register tables
    val start = System.currentTimeMillis()
    for (table <- tables) {
      //     val path = s"${conf.inputPath()}/${table}"
      val path = s"${conf.inputPath()}/${table}.parquet"
      println(s"Registering table $table at $path")
      val df = spark.read.parquet(path)
      df.createTempView(table)
      val duration = System.currentTimeMillis() - start
    }
    val duration = System.currentTimeMillis() - start
    w.write(s"Register Tables,$duration\n")
    w.flush()

    if (conf.query.isSupplied) {
      execute(spark, conf.queryPath(), conf.query().toInt, w)
    } else {
      for (query <- 1 to 99) {
        if (query == 72 && conf.skipQuery72()) {
          // skip q72 on most runs because it takes forever without join reordering enabled
          println("Skipping query 72")
        } else {
          try {
            execute(spark, conf.queryPath(), query, w)
          } catch {
            case e: Exception =>
              // don't stop on errors
              println(s"Query $query FAILED:")
              e.printStackTrace()
          }
        }
      }
    }

    w.close()

    if (conf.keepAlive()) {
      println("Sleeping to keep driver alive ... ")
      Thread.sleep(Long.MaxValue)
    }
  }

  private def execute(spark: SparkSession, path: String, query: Int, w: BufferedWriter) {
    val sqlFile = s"$path/q$query.sql"
    println(s"Executing query $query from $sqlFile")

    val source = Source.fromFile(sqlFile)
    val sql = source.getLines.mkString("\n")
    source.close()

    val queries = sql.split(';').filterNot(_.trim.isEmpty)

    for ((sql, i) <- queries.zipWithIndex) {
      println(sql)

      val start = System.currentTimeMillis()
      val resultDf = spark.sql(sql)
      val results = resultDf.collect()
      val duration = System.currentTimeMillis() - start
      println(s"Query $query took $duration ms")

      var prefix = s"q$query"
      if (queries.length > 1) {
        prefix += "_part_" + (i+1)
      }
      w.write(s"$prefix,$duration\n")
      w.flush()

      val optimizedLogicalPlan = resultDf.queryExecution.optimizedPlan
      writeFile(prefix, "logical_plan.txt", optimizedLogicalPlan.toString())
      writeFile(prefix, "logical_plan.qpml", Qpml.fromLogicalPlan(optimizedLogicalPlan))
      val physicalPlan = resultDf.queryExecution.executedPlan
      writeFile(prefix, "physical_plan.txt", physicalPlan.toString())

      val csvFilename = s"$prefix.csv"

      // write results to CSV format
      val resultWriter = new BufferedWriter(new FileWriter(csvFilename))
      resultWriter.write(physicalPlan.schema.fieldNames.mkString(",") + "\n")
      results.foreach(row => resultWriter.write(row.mkString(",") + "\n"))
      resultWriter.close()

      // could also save directly from dataframe but this would execute the query again
      //resultDf.coalesce(1).write.csv(csvFilename)
    }
  }

  def writeFile(prefix: String, suffix: String, text: String): Unit = {
    val filename = prefix + "_" + suffix
    println(s"Writing $filename")
    val file = new File(filename)
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write(text)
    bw.close()
  }

}
