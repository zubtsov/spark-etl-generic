package org.zubtsov.spark.etl1.builder

import com.zubtsov.spark.api.Table
import com.zubtsov.spark.api.writing.Save
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, row_number}
import org.zubtsov.spark.etl1.reader.MyReader
import org.zubtsov.spark.etl1.writer.MyWriter

class Customer {
  @Save(writer = MyWriter.Name)
  @Table(Customer.TableName)
  def build(@Table(name = "customer_raw", reader = MyReader.Name) customerRaw: DataFrame): DataFrame = {
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