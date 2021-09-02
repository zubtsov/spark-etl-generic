package org.zubtsov.spark.etl1.writer

import com.zubtsov.spark.api.configuration.Configuration
import com.zubtsov.spark.api.writing.TableWriter
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