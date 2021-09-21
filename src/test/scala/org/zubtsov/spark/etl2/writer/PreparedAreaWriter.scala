package org.zubtsov.spark.etl2.writer

import com.zubtsov.spark.api.annotation.configuration.Configuration
import com.zubtsov.spark.api.annotation.writing.TableWriter
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

class PreparedAreaWriter {
  @TableWriter(name = PreparedAreaWriter.Name)
  def write(tableName: String, dataFrame: DataFrame, conf: Configuration[String]): DataFrame = {
    val preparedAreaPath = conf.get("prepared_area_path")
    val tablePath = s"${preparedAreaPath}/${tableName}"

    dataFrame.coalesce(1).write.mode(SaveMode.Overwrite).format("csv").option("header", true).save(tablePath)

    val readTable = SparkSession.active.read.format("csv").option("header", true).load(tablePath)
    readTable.show()
    readTable
  }
}

object PreparedAreaWriter {
  final val Name = "PreparedAreaWriter"
}