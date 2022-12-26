package io.andygrove.sqlbenchds

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.internal.SQLConf
import org.rogach.scallop.ScallopConf

import java.io.{BufferedWriter, File, FileWriter}
import scala.collection.mutable.ListBuffer
import scala.io.Source

class Conf(args: Array[String]) extends ScallopConf(args) {
  val inputPath = opt[String](required = true)
  val queryPath = opt[String](required = true)
  val query = opt[String](required = true)
  val iterations = opt[Int](required = false, default = Some(1))
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

    val sqlFile = s"${conf.queryPath()}/${conf.query()}.sql"
    val source = Source.fromFile(sqlFile)
    val sql = source.getLines.mkString("\n")
    source.close()
    println(sql)

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

    val timing = new ListBuffer[Long]()
    for (i <- 0 until conf.iterations()) {
      println(s"Iteration $i")
      val start = System.currentTimeMillis()
      val resultDf = spark.sql(sql)


      val optimizedLogicalPlan = resultDf.queryExecution.optimizedPlan
      writeFile(conf.query(), "logical-plan.txt", optimizedLogicalPlan.toString())
      writeFile(conf.query(), "logical-plan.qpml", Qpml.fromLogicalPlan(optimizedLogicalPlan))

      val results = resultDf.collect()
      results.foreach(println)

      val physicalPlan = resultDf.queryExecution.executedPlan
      writeFile(conf.query(), "physical-plan.txt", physicalPlan.toString())

      val duration = System.currentTimeMillis() - start
      println(s"Iteration $i took $duration ms")
      timing += duration
    }

    // summarize the results
    timing.zipWithIndex.foreach {
      case (n, i) => println(s"Iteration $i took $n ms")
    }

//    println("Sleeping to keep driver alive ... ")
//    Thread.sleep(Long.MaxValue)
  }


  def writeFile(query: String, suffix: String, text: String): Unit = {
    val filename = s"q$query-$suffix"
    println(s"Writing $filename")
    val file = new File(filename)
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write(text)
    bw.close()
  }

}
