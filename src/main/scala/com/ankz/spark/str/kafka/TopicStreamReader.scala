package com.ankz.spark.str.kafka

import org.apache.spark.sql.SparkSession
import java.sql.Timestamp
import org.apache.spark.sql.streaming.OutputMode

object TopicStreamReader {

  println("In TopicStreamReader")
  System.setProperty("hadoop.home.dir", "D:/Setup/Hadoop/hadoop-3.0.0");

  def main(args: Array[String]): Unit = {

    val bootstrapserver = "localhost:9092"
    val topic = "kafkastream"

    val spark = SparkSession.builder.
      master("local")
      .appName("spark TopicReader example")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val kafkaDF = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrapserver)
      .option("subscribe", topic)
      .load()

    println("kafkaDF.printSchema : " + kafkaDF.printSchema)

    import spark.implicits._
    val kafkaDS = kafkaDF.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "CAST(timestamp AS TIMESTAMP)").as[(String, String, Timestamp)]
    println("kafkaDS.printSchema : " + kafkaDS.printSchema)
    /*   val splittedDF = topicDF.selectExpr(
      "split(value,',')[0] as transactionId",
      "split(value,',')[1] as customerId",
      "split(value,',')[2] as itemId",
      "split(value,',')[3] as amountPaid",
      "timestamp as timestamp")
       val results = splittedDF.groupBy(window($"timestamp", "60 seconds")).agg(avg("amountPaid")
      .alias("average_sales"))
      .select("window.start", "window.end", "average_sales")
*/
    val query = kafkaDS.writeStream
      .format("console")
      .outputMode(OutputMode.Append)
      .option("truncate", "false")
      .queryName("TopicStreamReader")
      .start()

    query.awaitTermination()
    query.stop()

  }

}