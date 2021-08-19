package org.zubtsov.spark.etl1.builder

import com.zubtsov.spark.api.Table
import com.zubtsov.spark.api.writing.Save
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, row_number}
import org.zubtsov.spark.etl1.reader.MyReader
import org.zubtsov.spark.etl1.writer.MyWriter

class Product {
  @Save(writer = MyWriter.Name)
  @Table(Product.TableName)
  def build(@Table(name = "product_raw", reader = MyReader.Name) productRaw: DataFrame): DataFrame = {
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