package com.action

import com.transformation.{ProductType, Revenue}

object action {
  def main(args:Array[String]): Unit ={
    val producttype = new ProductType()
    producttype.productTypeCount()
    
    val revenue = new Revenue()
    revenue.totalRevenue()
  }
}
