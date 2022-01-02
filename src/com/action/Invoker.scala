package com.action

import com.transformation.{ProductTypeImpl, RevenueImpl}


object Invoker {
  def main(args:Array[String]): Unit ={
    val productType = new ProductTypeImpl()
    println("No of Product Type under Golf Equipment : " +productType.productTypeCount("Golf Equipment"))
    val revenue = new RevenueImpl()
    println("Total revenue for the Retailer Country France is :"+revenue.totalRevenue("France"))
  }

}
