package com.action

import com.`enum`.ProductLine
import com.transformation.{ProductTypeImpl, RevenueImpl}


object Invoker {
  def main(args:Array[String]): Unit ={
    val productType = new ProductTypeImpl()
    val count = productType.productTypeCount(ProductLine.GolfEquipment);
    println("No of Product Type under Golf Equipment : " + count);

    val revenue = new RevenueImpl()
    val countryRevenue = revenue.totalRevenue("France")
    println("Total revenue for the Retailer Country France is :" + countryRevenue)

  }

}
