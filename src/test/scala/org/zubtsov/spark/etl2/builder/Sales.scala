package org.zubtsov.spark.etl2.builder

import com.zubtsov.spark.api.annotation.Table
import com.zubtsov.spark.api.annotation.writing.{MultipleSave, Save}
import org.apache.spark.sql.DataFrame
import org.zubtsov.spark.etl2.{reader, writer}

class Sales {
  @MultipleSave(Array(
    new Save(writer = writer.PreparedAreaWriter.Name),
    new Save(writer = writer.PreparedAreaWriter.Name)
  ))
  @Table(Sales.TableName)
  def build(
            @Table(name = Organization.TableName)
            organization: DataFrame,
            @Table(name = Product.TableName)
            product: DataFrame,
            @Table(name = Customer.TableName)
            customer: DataFrame,
            @Table(name = "sales_raw", reader = reader.RawAreaReader.Name)
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