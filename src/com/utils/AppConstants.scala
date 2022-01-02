package com.utils

object AppConstants {
  val IS_DEV = true; // when building jar make it false
  val APP_NAME = "SALES_DATA"
  val SPARK_MASTER = if(IS_DEV) "local[*]" else "spark://balakrish:7077" // add ur spark URI
  val SALES_FILE_PATH = if(IS_DEV) "sales.csv" else "hdfs://localhost:9000/data/sales.csv"  // add ur hadoop path

}
