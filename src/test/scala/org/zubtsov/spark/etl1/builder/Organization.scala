package org.zubtsov.spark.etl1.builder

import com.zubtsov.spark.api.Table
import com.zubtsov.spark.api.writing.Save
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, row_number}
import org.zubtsov.spark.etl1.reader.MyReader
import org.zubtsov.spark.etl1.writer.MyWriter

class Organization {
  @Save(writer = MyWriter.Name)
  @Table(Organization.TableName)
  def build(@Table(name = "organization_raw", reader = MyReader.Name) organizationRaw: DataFrame): DataFrame = {
    organizationRaw
      .select(
        row_number().over(Window.orderBy("organization_id")).as("id"), //FIXME: it's a bad approach of generating IDs
        col("organization_id").as("legacy_id"),
        col("organization_name").as("name")
      )
  }
}

object Organization {
  final val TableName = "organization"
}