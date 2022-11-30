package com.ankz.spark.str


import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.OutputMode


/**
 * @author ankit.kamal
 * Reading lines from socket-stream using NC and computing wordCount.
 */
object WordCount {

  println("In WordCount :")
  System.setProperty("hadoop.home.dir", "D:/Setup/Hadoop/hadoop-3.0.0");


  val checkpointLocation = "D:/Setup/spark_cpt";

  def main(args: Array[String]): Unit = {

    println("In WordCount")

    //getting Spark-Session
    val spark = SparkSession
      .builder
      .master("local[2]")
      .appName("StructuredNetworkWordCount")
      .getOrCreate()

    //       spark.sparkContext.setLogLevel("INFO")
    spark.sparkContext.setLogLevel("ERROR")


    //reading stream from socket-source
    val lines = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .load()

    println(lines.printSchema)
    //      or
    val lines2 = lines.selectExpr("CAST(value AS STRING)")

    println(lines2.printSchema)

    /* OP for lines2
     *
-------------------------------------------
Batch: 1
-------------------------------------------
+-------+
|  value|
+-------+
|Hi User|
+-------+

-------------------------------------------
Batch: 2
-------------------------------------------
+-------------+
|        value|
+-------------+
|Hi I am user2|
+-------------+
-------------------------------------------
Batch: 1
-------------------------------------------
+-------+
|  value|
+-------+
|Hi User|
+-------+

-------------------------------------------
Batch: 2
-------------------------------------------
+-------------+
|        value|
+-------------+
|Hi I am user2|
+-------------+
     */

    import spark.implicits._
    // Split the lines into words
    val words = lines.as[String].flatMap(_.split(" "))

    // Generate running word count
    val wordCounts = words.groupBy("value").count()

    // Start running the query that prints the running counts to the console
    val query = wordCounts.writeStream
      .outputMode(OutputMode.Complete)
      .format("console")
      .start()

    query.awaitTermination()

  }
}

/* OP
Hi I am user1
Hi I am user2
user2


-------------------------------------------
Batch: 1
-------------------------------------------
+-----+-----+
|value|count|
+-----+-----+
|user1|    1|
|   Hi|    1|
|    I|    1|
|   am|    1|
+-----+-----+

-------------------------------------------
Batch: 2
-------------------------------------------
+-----+-----+
|value|count|
+-----+-----+
|   Hi|    2|
|user2|    1|
|    I|    2|
|   am|    2|
+-----+-----+

-------------------------------------------
Batch: 3
-------------------------------------------
+-----+-----+
|value|count|
+-----+-----+
|user2|    2|
+-----+-----+

*/