package com.transformation

import com.utils.{Utility}


class RevenueImpl {
  def totalRevenue(): Double ={
    val sparkSS= new Utility()
    val ss = sparkSS.sparkSessionBuilder
    val sparkContext = sparkSS.sparkSS()
    sparkSS.readCSV()
    val salesDF = ss.sql("SELECT Revenue FROM SALES_TABLE where RetailerCountry= 'France' and Revenue != '0'")
    val salesRDD = salesDF.rdd
    val revenueAccumulator = sparkContext.doubleAccumulator("revenueAccumulator")
    salesRDD.foreach(each =>
      revenueAccumulator.add {
        each.getString(0).toDouble
      }
    )
    
    println("Total revenue for the Retailer Country France is :"+revenueAccumulator.value)
  }
}
