package com.transformation

import com.utils.{Utility}


class RevenueImpl extends Utility{
  def totalRevenue(Countryname:String): Double ={
    val sparkSS= new Utility()
    val ss = sparkSS.sparkSessionBuilder
    val sparkContext = sparkSS.sparkSS()
    sparkSS.readCSV()
    val salesDF = ss.sql("SELECT Revenue FROM SALES_TABLE where RetailerCountry= '"+Countryname+"' and Revenue != '0'")
    val salesRDD = salesDF.rdd
    val revenueAccumulator = sparkContext.doubleAccumulator("revenueAccumulator")
    salesRDD.foreach(each =>
      revenueAccumulator.add {
        each.getString(0).toDouble
      }
    )
    val totalRevenue = revenueAccumulator.value
    totalRevenue
  }
}
