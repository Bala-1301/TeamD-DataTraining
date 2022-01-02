package com.transformation

import com.utils.{AppConstants, Utility}

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
    try {
      val fileObject = new File(AppConstants.FILE_OUT_DIR)
      val printWriter = new PrintWriter(fileObject)
      printWriter.write("Total revenue for the Retailer Country France is :"+totalRevenue)
      printWriter.close()
    } catch {
      case e: Exception => println("Error! Can't write to output file")
    }

    totalRevenue
  }
}
