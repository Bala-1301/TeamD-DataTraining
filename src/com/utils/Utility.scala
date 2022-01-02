package com.utils
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import scala.io.Source
class Utility {
    System.setProperty("hadoop.home.dir", "D:\\data\\winutils\\")
    def sparkSessionBuilder: SparkSession ={
        val ss = SparkSession.builder()
          .appName("CodaDataJOB")
          .master("local[*]")
          .getOrCreate()
        ss
    }
    def sparkSS(): SparkContext ={
        val ss = sparkSessionBuilder
        val sparkContext = ss.sparkContext
        sparkContext
    }
    def readCSV(): Unit ={
        val ss = sparkSessionBuilder
        import ss.implicits._
        val csvRead=for {
            line <- Source.fromFile("sales.csv").getLines().drop(1).toVector
            values = line.split(",")
        } yield Sale(values(0), values(1), values(2), values(3), values(4),values(5),values(6),values(7),values(8),values(9),values(10),values(11),values(12),values(13))
        val csvDF = csvRead.toDF()
        csvDF.createOrReplaceTempView("SALES_TABLE")
    }
}
case class Sale( Year: String,
                 Productline: String,
                 Producttype: String,
                 Product: String,
                 Ordermethodtype: String,
                 Retailercountry:String,
                 Revenue:String,
                 Plannedrevenue:String,
                 Productcost:String,
                 Quantity:String,
                 Unitcost:String,
                 Unitprice:String,
                 Grossprofit:String,
                 Unitsaleprice:String)