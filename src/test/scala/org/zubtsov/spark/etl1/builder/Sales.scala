package org.zubtsov.spark.etl1.builder

import com.zubtsov.spark.api.Table
import com.zubtsov.spark.api.writing.{MultipleSave, Save}
import org.apache.spark.sql.DataFrame
import org.zubtsov.spark.etl1.reader.RawAreaReader
import org.zubtsov.spark.etl1.writer.PreparedAreaWriter

class Sales {
  @MultipleSave(Array(
    new Save(writer = PreparedAreaWriter.Name),
    new Save(writer = PreparedAreaWriter.Name)
  ))
  @Table(Sales.TableName)
  def build(
            @Table(name = Organization.TableName)
            organization: DataFrame,
            @Table(name = Product.TableName)
            product: DataFrame,
            @Table(name = Customer.TableName)
            customer: DataFrame,
            @Table(name = "sales_raw", reader = RawAreaReader.Name)
            salesRaw: DataFrame
           ): DataFrame = {
    salesRaw
      .join(organization, salesRaw("organization_legacy_id") === organization("legacy_id"))
      .join(product, salesRaw("product_legacy_id") === product("legacy_id"))
      .join(customer, salesRaw("customer_legacy_id") === customer("legacy_id"))
      .select(
        organization("id").as("organization_id"),
        product("id").as("product_id"),
        customer("id").as("customer_id"),
        salesRaw("quantity"),
        salesRaw("unit_price")
      )
  }
}

object Sales {
  final val TableName = "sales"
}