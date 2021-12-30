package com.utils

import org.apache.spark.sql.SparkSession

trait Utility {
    System.setProperty("hadoop.home.dir", "D:\\winutils\\")
    val ss = SparkSession.builder()
      .appName("CodaDataJOB")
      .master("local[*]")
      .getOrCreate()

    val sparkContext = ss.sparkContext

    val path="D:\\Big Data\\scalatrainingintellij-master\\data\\sales.csv"

    val csvDF = ss.read
      .option("header", "true")
      .csv(path)

    csvDF.createOrReplaceTempView("SALES_TABLE")
}
