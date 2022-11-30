package com.ankz.spark.str



import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.functions._


/**
 * @author ankit.kamal
 * Reading data from csv-file-stream  computing count of custIds.
 * This is to demonstrate output modes(with/without aggregations)
 */
object csvFileStreamReader {



  println("In csvStreamReader : Reading csv files as Stream")
  System.setProperty("hadoop.home.dir", "D:/Setup/Hadoop/hadoop-3.0.0");


   val csvInputPath = "D:\\Demos\\Spark\\test_data_streaming\\inputDir"
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder.
      master("local")
      .appName("spark csv File example")
      .getOrCreate()

    //    spark.sparkContext.setLogLevel("DEBUG")
    spark.sparkContext.setLogLevel("ERROR")

    //spark.sql.shuffle.partitions which is by default set to 200.
    spark.conf.set("spark.sql.shuffle.partitions",5)


    //1. custom-schema using StructType
    val userSchema = StructType(
      StructField("transactionId", LongType, false) ::
        StructField("customerId", LongType, false) ::
        StructField("itemId", LongType, false) ::
        StructField("amountPaid", LongType, false) :: Nil)

    val csvDF = spark
      .readStream
      .option("sep", ",")
      .schema(userSchema)
      .option("header", true)
      .csv(csvInputPath)

    println("csvDF.printSchema : " + csvDF.printSchema)

    //  --------------- OR ---------------

    //2. custom-schema using CaseClass
    val userSchemaCC = Encoders.product[SalesData].schema
    val csvDF2 = spark
      .readStream
      .schema(userSchemaCC)
      .option("header", true)
      .csv(csvInputPath)

    println("csvDF2.printSchema : " + csvDF2.printSchema)

    //        val query = topicDF0.writeStream
    /*   val query = topicDF.writeStream
         .format("console")
         //      .outputMode(OutputMode.Append)//def
         .outputMode(OutputMode.Update)
         //      .outputMode(OutputMode.Complete)
         .option("truncate", "false")
         .queryName("query")
         .start()
         .awaitTermination()*/

    //groupBy custId and get Total Count
    import spark.implicits._
    val custCount = csvDF2
      .groupBy("customerId")
      //      .agg(avg("customerId")
      .agg(count("customerId")
        .alias("custCount"))

    val countQuery = custCount.writeStream
      .format("console")
      //      .outputMode(OutputMode.Append)// reserved to the processing without any aggregation(immutable results)
//            .outputMode(OutputMode.Update)
      .outputMode(OutputMode.Complete)
      .option("truncate", "false")
      .queryName("countQuery")
      .start()
      .awaitTermination()

    /*
    import scala.concurrent.duration._
    import org.apache.spark.sql.streaming.{ OutputMode, Trigger }
    val populationStream = custCount.
      writeStream.
      format("console").
      trigger(Trigger.ProcessingTime(30.seconds)).
      outputMode(OutputMode.Complete).
      queryName("custCount").
      start

    populationStream.awaitTermination()*/
  }

  case class SalesData(transactionId: Long, customerId: Long, itemId: Long, amountPaid: Long)



}
/*
 * OP >>>
Reading all the CSVs from path = csvInputPath
OutputMode.Complete
Batch: 0
-------------------------------------------
+----------+---------+
|customerId|custCount|
+----------+---------+
|105       |1        |
|102       |1        |
|101       |1        |
|103       |1        |
|104       |1        |
+----------+---------+

-------------------------------------------
Batch: 1
-------------------------------------------
+----------+---------+
|customerId|custCount|
+----------+---------+
|105       |2        |
|102       |2        |
|101       |2        |
|103       |2        |
|104       |2        |
+----------+---------+

-------------------------------------------
Batch: 2
-------------------------------------------
+----------+---------+
|customerId|custCount|
+----------+---------+
|105       |2        |
|102       |4        |
|101       |5        |
|103       |2        |
|104       |2        |
+----------+---------+

 */


/*

* OP >>>
Reading all the CSVs from path = csvInputPath
OutputMode.Update
-------------------------------------------
Batch: 0
-------------------------------------------
+----------+---------+
|customerId|custCount|
+----------+---------+
|105       |1        |
|102       |1        |
|101       |1        |
|103       |1        |
|104       |1        |
+----------+---------+

-------------------------------------------
Batch: 1
-------------------------------------------
+----------+---------+
|customerId|custCount|
+----------+---------+
|105       |2        |
|102       |2        |
|101       |2        |
|103       |2        |
|104       |2        |
+----------+---------+

-------------------------------------------
Batch: 2
-------------------------------------------
+----------+---------+
|customerId|custCount|
+----------+---------+
|102       |4        |
|101       |5        |
+----------+---------+
 */