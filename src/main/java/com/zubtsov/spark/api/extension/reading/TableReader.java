package com.zubtsov.spark.api.extension.reading;

import com.zubtsov.spark.api.extension.configuration.Configuration;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static com.zubtsov.spark.common.DefaultNames.DEFAULT_READER_NAME;

public interface TableReader<T> {
    default String getName() {
        return DEFAULT_READER_NAME;
    }

    Dataset<Row> readTable(String tableName, Configuration<T> configuration);
}
