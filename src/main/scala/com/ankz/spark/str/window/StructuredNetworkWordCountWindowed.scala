package com.ankz.spark.str.window

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.OutputMode
import java.sql.Timestamp
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Dataset

object StructuredNetworkWordCountWindowed {

  println("In StructuredNetworkWordCountWindowed")
 System.setProperty("hadoop.home.dir", "D:/Setup/Hadoop/hadoop-3.0.0");


  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder.
      master("local[*]")
      .appName("StructuredNetworkWordCountWindowed")
      .getOrCreate()

    //    spark.sparkContext.setLogLevel("DEBUG")
    spark.sparkContext.setLogLevel("ERROR")

    //reading stream from socket-source
    val lines = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .option("includeTimestamp", true)
      .load()

    //    spark.sparkContext.setLogLevel("DEBUG")
    spark.sparkContext.setLogLevel("ERROR")
    spark.conf.set("spark.sql.shuffle.partitions",5)


    println(lines.printSchema)
//    lines.explain(true);
    //      or
    val lines2 = lines.selectExpr("CAST(value AS STRING)")

    import spark.implicits._
    //    val lines2 = lines.selectExpr("CAST(value AS STRING)").as[(String)]
    println(lines2.printSchema)


    /*    val query_lines = lines.writeStream
          .format("console")
          .outputMode(OutputMode.Append)
          .option("truncate", "false")
          .queryName("query_lines")
          .start()
    */
    //    val words = lines.as[String].flatMap(_.split(" "))
    // Split the lines into words, retaining timestamps
    val wordsDS: Dataset[(String, Timestamp)] = lines.as[(String, Timestamp)]
      .flatMap(line =>
        line._1.split(" ").map(word => (word, line._2)))

    val wordsDF = wordsDS.toDF("word", "timestamp")

    // Generate running word count
    //    val wordCounts = words.groupBy("value").count()

    // Group the data by window and word and compute the count of each group

    val windowDuration = "10 seconds"
    val slideDuration = "5 seconds"

    val windowedCounts = wordsDF.groupBy(
      window($"timestamp", windowDuration), $"word").count().orderBy("window")

    /*    val windowedCounts = wordsDF.groupBy(
          window($"timestamp", windowDuration, slideDuration), $"word").count().orderBy("window")*/

//    windowedCounts.explain(true)

    //    val query_lines2 = windowedCounts.writeStream
    val query_windowedCounts = windowedCounts.writeStream
      .format("console")
      .outputMode(OutputMode.Complete)
      .option("truncate", "false")
      .queryName("query_windowedCounts")
      .start()


    spark.streams.awaitAnyTermination()

  }

}