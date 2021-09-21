package com.zubtsov.spark.api.extension.reading;

import com.zubtsov.spark.api.extension.configuration.Configuration;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public interface  TableReader<T> {
    String getName();
    Dataset<Row> readTable(String tableName, Configuration<T> configuration);
}
