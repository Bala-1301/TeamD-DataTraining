package com.transformation

import com.utils.Utility

import java.io.{File, PrintWriter}



class RevenueImpl {
  def totalRevenue(countryName: String): Double ={
    val sparkSS= new Utility()
    val ss = sparkSS.sparkSessionBuilder
    val sparkContext = sparkSS.sparkSS()
    sparkSS.readCSV()
    val salesDF = ss.sql("SELECT Revenue FROM SALES_TABLE where RetailerCountry= '" +
      countryName + "' and Revenue != '0'")
    val salesRDD = salesDF.rdd
    val revenueAccumulator = sparkContext.doubleAccumulator("revenueAccumulator")
    salesRDD.foreach(each =>
      revenueAccumulator.add {
        each.getString(0).toDouble
      }
    )

    val totalRevenue = revenueAccumulator.value
    val fileObject = new File("D:\\data\\TotalRevenue.txt")
    val printWriter = new PrintWriter(fileObject)
    printWriter.write("Total revenue for the Retailer Country France is :"+totalRevenue)
    printWriter.close()
    totalRevenue
  }
}
