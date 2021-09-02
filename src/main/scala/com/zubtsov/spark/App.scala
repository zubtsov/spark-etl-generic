package com.zubtsov.spark

import org.apache.spark.sql.SparkSession

/**
 * @author ${user.name}
 */
object App {
  
  def foo(x : Array[String]) = x.foldLeft("")((a,b) => a + b)
  
  def main(args : Array[String]) {
    println( "Hello World!" )
    println("concat arguments = " + foo(args))
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .getOrCreate()

    spark.emptyDataFrame.show()
  }

}
