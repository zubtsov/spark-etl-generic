package org.zubtsov.spark.etl1.builder

import com.zubtsov.spark.api.annotation.Table
import com.zubtsov.spark.api.annotation.writing.Save
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, row_number}
import org.zubtsov.spark.etl1.reader.RawAreaReader
import org.zubtsov.spark.etl1.writer.PreparedAreaWriter

class Product {
  @Save(writer = PreparedAreaWriter.Name)
  @Table(Product.TableName)
  def build(@Table(name = "product_raw", reader = RawAreaReader.Name) productRaw: DataFrame): DataFrame = {
    productRaw
      .select(
        row_number().over(Window.orderBy("product_id")).as("id"), //FIXME: it's a bad approach of generating IDs
        col("product_id").as("legacy_id"),
        col("product_name").as("name")
      )
  }
}

object Product {
  final val TableName = "product"
}