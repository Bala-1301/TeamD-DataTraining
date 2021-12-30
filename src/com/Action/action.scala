package com.Action

import com.Transformation.ProductType

object action {
  def main(args:Array[String]): Unit ={
    val producttype = new ProductType()
    producttype.productTypeCount()
  }
}
