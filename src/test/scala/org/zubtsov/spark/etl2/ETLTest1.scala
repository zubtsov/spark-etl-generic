package org.zubtsov.spark.etl2

import com.zubtsov.spark.api.annotation.configuration.Configuration
import com.zubtsov.spark.model.SparkBatchJob
import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite

class ETLTest1 extends FunSuite {
  test("ETL 1") {
    SparkSession.builder().master("local[*]").getOrCreate()

    val configuration: Configuration[String] = (key: String) => Map(
      "raw_area_path" -> "src/test/resources/test-data/etl1",
      "prepared_area_path" -> "target/test-data/etl1",
    )(key)

    new SparkBatchJob("org.zubtsov.spark.etl1")
      .run(configuration);
  }
}
