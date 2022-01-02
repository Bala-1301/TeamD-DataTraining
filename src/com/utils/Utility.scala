package com.utils

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

class Utility {
    //returns spark session
    def sparkSessionBuilder: SparkSession = {
        if(AppConstants.IS_DEV)
            System.setProperty("hadoop.home.dir", "D:\\winutils\\")
        val ss = SparkSession.builder()
          .appName(AppConstants.APP_NAME)
          .master(AppConstants.SPARK_MASTER)
          .getOrCreate()
        ss
    }

    //returns spark context
    def sparkSS(): SparkContext ={
        val ss = sparkSessionBuilder
        val sparkContext = ss.sparkContext
        sparkContext
    }

    //function to read csv file
    def readCSV(): Unit = {
        val ss = sparkSessionBuilder
        val salesDS = ss.read.option("header", "true").csv(AppConstants.SALES_FILE_PATH)
        salesDS.toDF().createOrReplaceTempView("SALES_TABLE")
    }
}

case class Sale( Year: String,
                 ProductLine: String,
                 ProductType: String,
                 Product: String,
                 OrderMethodType: String,
                 RetailerCountry:String,
                 Revenue:String,
                 PlannedRevenue:String,
                 ProductCost:String,
                 Quantity:String,
                 UnitCost:String,
                 UnitPrice:String,
                 GrossProfit:String,
                 UnitSalePrice:String)


