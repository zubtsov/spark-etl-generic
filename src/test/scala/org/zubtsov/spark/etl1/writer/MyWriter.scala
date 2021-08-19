package org.zubtsov.spark.etl1.writer

import com.zubtsov.spark.api.writing.TableWriter
import org.apache.spark.sql.DataFrame

class MyWriter {
  @TableWriter(name = MyWriter.Name)
  def write(tableName: String, dataFrame: DataFrame): DataFrame = {
    dataFrame.show()
    dataFrame
  }
}

object MyWriter {
  final val Name = "MyWriter"
}