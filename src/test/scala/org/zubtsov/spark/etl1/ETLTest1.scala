package org.zubtsov.spark.etl1

import com.zubtsov.spark.model.SparkBatchJob
import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite

class ETLTest1 extends FunSuite {
  test("ETL 1") {
    SparkSession.builder().master("local[*]").getOrCreate()
    new SparkBatchJob("org.zubtsov.spark.etl1").run();
  }
}
