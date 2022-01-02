package com.transformation

import com.utils.Utility

class ProductTypeImpl extends Utility  {
  def productTypeCount(Productname:String): Long ={
    val sparkSS= new Utility()
    val ss = sparkSS.sparkSessionBuilder
    sparkSS.readCSV()
    val salesDF = ss.sql("SELECT Producttype FROM SALES_TABLE where Productline= '"+Productname+"'")
    val proTypeRDD = salesDF.rdd
    val mappedRDD = proTypeRDD.map(eachWord => (eachWord,1))
    val finalMap = mappedRDD.reduceByKey((x,y) => x+y)
    val typeCount = finalMap.count()
    typeCount
  }

}
