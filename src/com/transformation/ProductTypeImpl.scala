package com.transformation

import com.utils.Utility

class ProductType extends Utility  {
  def productTypeCount(): Unit ={
    val salesDF = ss.sql("SELECT Producttype FROM SALES_TABLE where Productline= 'Golf Equipment'")

    val proTypeRDD = salesDF.rdd

    val mappedRDD = proTypeRDD.map(eachWord => (eachWord,1))

    val finalMap = mappedRDD.reduceByKey((x,y) => x+y)

    println("No of Product Type under Golf Equipment : " + finalMap.count())
  }

}
