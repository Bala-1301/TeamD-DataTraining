package com.action

import com.transformation.{ProductTypeImpl, RevenueImpl}


object action {
  def main(args:Array[String]): Unit = {

    val productType = new ProductTypeImpl()
    println("No of Product Type under Golf Equipment : " +productType.productTypeCount())

    val revenue = new RevenueImpl()
    println("Total revenue for the Retailer Country France is :"+revenue.totalRevenue())
  }
}
