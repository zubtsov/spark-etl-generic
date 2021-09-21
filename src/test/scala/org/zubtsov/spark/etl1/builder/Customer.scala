package org.zubtsov.spark.etl1.builder

import com.zubtsov.spark.api.annotation.Table
import com.zubtsov.spark.api.annotation.writing.Save
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, row_number}
import org.zubtsov.spark.etl1.reader.RawAreaReader
import org.zubtsov.spark.etl1.writer.PreparedAreaWriter

class Customer {
  @Save(writer = PreparedAreaWriter.Name)
  @Table(Customer.TableName)
  def build(@Table(name = "customer_raw", reader = RawAreaReader.Name) customerRaw: DataFrame): DataFrame = {
    customerRaw
      .select(
        row_number().over(Window.orderBy("customer_id")).as("id"), //FIXME: it's a bad approach of generating IDs
        col("customer_id").as("legacy_id"),
        col("customer_name").as("name")
      )
  }
}

object Customer {
  final val TableName = "customer"
}