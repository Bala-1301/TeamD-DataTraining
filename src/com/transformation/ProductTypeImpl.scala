package com.transformation

import com.`enum`.ProductLine.ProductLine
import com.utils.Utility
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import scala.swing.Table.ElementMode

class ProductTypeImpl extends Utility  {

  def productTypeCount(productLine: ProductLine): Long ={
    //read csv
    val sparkSS= new Utility()
    val ss = sparkSS.sparkSessionBuilder
    sparkSS.readCSV()

    //query
    val salesDF = ss.sql("SELECT Producttype FROM SALES_TABLE where Productline= '" + productLine + "'")
    val proTypeRDD = salesDF.rdd
    val mappedRDD = proTypeRDD.map(eachWord => (eachWord,1))
    val finalMap = mappedRDD.reduceByKey((x,y) => x+y)
    val productTypeRow = finalMap.map(each => Row(each._1.toString()))

    //write output to a file
    val schema = StructType(Array(StructField("ProductTypes", StringType,true)))
    val productTypeDF = ss.createDataFrame(productTypeRow,schema)
    productTypeDF.write.parquet("D:\\data\\ProductTypeOutput")
    val typeCount = finalMap.count()
    typeCount
  }

}
