package com.Transformation

import com.utils.Utility
import org.apache.spark.sql.types.DoubleType

import scala.util._


class Revenue extends Utility{
  def totalRevenue(): Unit ={
    val salesDF = ss.sql("SELECT Revenue FROM SALES_TABLE where `Retailer Country`= 'France' and Revenue != 'null'")

    val salesRDD = salesDF.rdd

    val revenueAccumulator = sparkContext.doubleAccumulator("revenueAccumulator")

    val revenueList = List(salesRDD)
    salesRDD.foreach(
      each => println(each+" "+each.getString(0).toDouble)

    )
    //println(each.getDouble(0).getClass())/
    salesRDD.foreach(each =>
      revenueAccumulator.add {
        each.getString(0).toDouble
      }
    )
    println(revenueAccumulator.value)
  }
}