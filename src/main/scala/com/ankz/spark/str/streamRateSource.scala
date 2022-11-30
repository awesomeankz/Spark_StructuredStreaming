package com.ankz.spark.str

import org.apache.spark.sql.{SparkSession}
import org.apache.spark.sql.functions._


/**
 * Rate source will auto-generate data which we will then print onto a console.
 */
object streamRateSource {


  println("In streamRateSource : Reading rate source as Stream")
  System.setProperty("hadoop.home.dir", "D:/Setup/Hadoop/hadoop-3.0.0");



  def main(args: Array[String]): Unit = {

    // Create Spark Session
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("Rate Source")
      .getOrCreate()

    // Set Spark logging level to ERROR to avoid various other logs on console.
    spark.sparkContext.setLogLevel("ERROR")

    // Create Streaming DataFrame by reading data from socket.
    val initDF = (spark
      .readStream
      .format("rate")
      .option("rowsPerSecond", 1)
      .load()

      )
    // Check if DataFrame is streaming or Not.
    println("Streaming DataFrame : " + initDF.isStreaming)

    // Print Schema of DataFrame
    println("Schema : " + initDF.printSchema())

    val resultDF = initDF
      .withColumn("result", col("value") + lit(1))


    /*
    Complete output mode not supported when there are no streaming aggregations on streaming DataFrames/Datasets;;
    Append output mode not supported when there are streaming aggregations on streaming DataFrames/DataSets without watermark;;
     */

//    resultDF
    initDF
      .writeStream
//      .outputMode("append")
      .outputMode("complete")
      .option("truncate", false)
      .format("console")
      .start()
      .awaitTermination()
  }
}

/*

root
 |-- timestamp: timestamp (nullable = true)
 |-- value: long (nullable = true)


-------------------------------------------
Batch: 0
-------------------------------------------
+-----------------------+-----+
|timestamp              |value|
+-----------------------+-----+
|2022-11-30 10:36:04.388|0    |
+-----------------------+-----+

-------------------------------------------
Batch: 1
-------------------------------------------
+-----------------------+-----+
|timestamp              |value|
+-----------------------+-----+
|2022-11-30 10:36:05.388|1    |
|2022-11-30 10:36:06.388|2    |
|2022-11-30 10:36:07.388|3    |
|2022-11-30 10:36:08.388|4    |
|2022-11-30 10:36:09.388|5    |
+-----------------------+-----+

-------------------------------------------
Batch: 2
-------------------------------------------
+-----------------------+-----+
|timestamp              |value|
+-----------------------+-----+
|2022-11-30 10:36:10.388|6    |
|2022-11-30 10:36:11.388|7    |
|2022-11-30 10:36:12.388|8    |
|2022-11-30 10:36:13.388|9    |
+-----------------------+-----+

org.apache.spark.sql.AnalysisException:
Complete output mode not supported when there are no streaming aggregations on streaming DataFrames/Datasets;;

 */