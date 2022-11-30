package com.ankz.spark.batch

import org.apache.spark.sql.SparkSession

object createStaticDF {


  val csvInputPath = "D:\\Demos\\Spark\\test_data\\csv\\inputDir"

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("createStaticDF")
      .getOrCreate()

    //to limit the console output logs
    spark.sparkContext.setLogLevel("ERROR")


    val staticDF =  spark.read
                      .option("header", true)
                      .csv(csvInputPath)

    staticDF.printSchema()

    staticDF.show()
  }
}
