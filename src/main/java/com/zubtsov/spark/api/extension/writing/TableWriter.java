package com.zubtsov.spark.api.extension.writing;

import com.zubtsov.spark.api.extension.configuration.Configuration;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static com.zubtsov.spark.common.DefaultNames.DEFAULT_WRITER_NAME;

public interface TableWriter<T> {
    default String getName() {
        return DEFAULT_WRITER_NAME;
    }

    Dataset<Row> writeTable(String tableName, Dataset<Row> data, Configuration<T> configuration);
}
