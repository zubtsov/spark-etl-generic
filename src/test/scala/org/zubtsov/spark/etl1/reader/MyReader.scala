package org.zubtsov.spark.etl1.reader

import com.zubtsov.spark.api.reading.TableReader
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

class MyReader {
  @TableReader(name = MyReader.Name)
  def read(tableName: String): DataFrame = {
    tableName match {
      case "sales_raw" => SparkSession.active.createDataFrame(SparkSession.active.sparkContext.parallelize(Seq(
        Row("org1", "prod1", "cust1", 10, 5.0),
        Row("org2", "prod2", "cust2", 20, 10.0),
        Row("org3", "prod3", "cust3", 30, 15.0)
      )), StructType.fromDDL("organization_legacy_id string, product_legacy_id string, customer_legacy_id string, quantity int, unit_price double"))
      case "organization_raw" => SparkSession.active.createDataFrame(SparkSession.active.sparkContext.parallelize(Seq(
        Row("org1", "Organization 1"),
        Row("org2", "Organization 2"),
        Row("org3", "Organization 3")
      )), StructType.fromDDL("organization_id string, organization_name string"))
      case "product_raw" => SparkSession.active.createDataFrame(SparkSession.active.sparkContext.parallelize(Seq(
        Row("prod1", "Product 1"),
        Row("prod2", "Product 2"),
        Row("prod3", "Product 3")
      )), StructType.fromDDL("product_id string, product_name string"))
      case "customer_raw" => SparkSession.active.createDataFrame(SparkSession.active.sparkContext.parallelize(Seq(
        Row("cust1", "Customer 1"),
        Row("cust2", "Customer 2"),
        Row("cust3", "Customer 3")
      )), StructType.fromDDL("customer_id string, customer_name string"))
      case _ => SparkSession.active.emptyDataFrame
    }
  }
}

object MyReader {
  final val Name = "MyReader"
}