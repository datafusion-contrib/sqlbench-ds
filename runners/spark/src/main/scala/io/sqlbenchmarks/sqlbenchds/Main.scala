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
  val iterations = opt[Int](required = false, default = Some(1))
  val keepAlive = opt[Boolean](required = false)
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

    val spark: SparkSession = SparkSession.builder
      .appName("Spark TPC-DS Benchmarks")
      .config(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key, "-1")
      .getOrCreate()

    // register tables
    for (table <- tables) {
      //     val path = s"${conf.inputPath()}/${table}"
      val path = s"${conf.inputPath()}/${table}.parquet"
      val df = spark.read.parquet(path)
      df.createTempView(table)
    }

    if (conf.query().nonEmpty) {
      execute(spark, conf.queryPath(), conf.query().toInt, conf.iterations())
    } else {
      for (query <- 1 until 99) {
        execute(spark, conf.queryPath(), query, conf.iterations())
      }
    }

    if (conf.keepAlive()) {
      println("Sleeping to keep driver alive ... ")
      Thread.sleep(Long.MaxValue)
    }
  }

  private def execute(spark: SparkSession, path: String, query: Int, iterations: Int) {
    val sqlFile = s"$path/$query.sql"
    val source = Source.fromFile(sqlFile)
    val sql = source.getLines.mkString("\n")
    source.close()

    val queries = sql.split(';')

    for ((sql, i) <- queries.zipWithIndex) {
      println(sql)

      val start = System.currentTimeMillis()
      val resultDf = spark.sql(sql)
      val results = resultDf.collect()
      val duration = System.currentTimeMillis() - start
      println(s"Query $query took $duration ms")
      results.foreach(println)

      var prefix = s"q$query"
      if (queries.length > 1) {
        prefix += "_part_" + (i+1)
      }

      val optimizedLogicalPlan = resultDf.queryExecution.optimizedPlan
      writeFile(prefix, "logical-plan.txt", optimizedLogicalPlan.toString())
      writeFile(prefix, "logical-plan.qpml", Qpml.fromLogicalPlan(optimizedLogicalPlan))
      val physicalPlan = resultDf.queryExecution.executedPlan
      writeFile(prefix, "physical-plan.txt", physicalPlan.toString())
    }
  }

  def writeFile(prefix: String, suffix: String, text: String): Unit = {
    val filename = s"$prefix+$suffix"
    println(s"Writing $filename")
    val file = new File(filename)
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write(text)
    bw.close()
  }

}
