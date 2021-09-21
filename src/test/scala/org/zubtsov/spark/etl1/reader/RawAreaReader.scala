package org.zubtsov.spark.etl1.reader

import com.zubtsov.spark.api.annotation.configuration.Configuration
import com.zubtsov.spark.api.annotation.reading.TableReader
import org.apache.spark.sql.{DataFrame, SparkSession}

class RawAreaReader {
  @TableReader(name = RawAreaReader.Name)
  def read(tableName: String, conf: Configuration[String]): DataFrame = {
    val rawAreaPath = conf.get("raw_area_path")
    SparkSession.active.read.format("csv").option("header", true).load(s"${rawAreaPath}/${tableName}")
  }
}

object RawAreaReader {
  final val Name = "RawAreaReader"
}