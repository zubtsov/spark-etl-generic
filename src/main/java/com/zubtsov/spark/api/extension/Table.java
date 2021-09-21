package com.zubtsov.spark.api.extension;

public interface Table {
    String getName();

    String getReaderName();

    default String[] getReadersNames() {
        return new String[]{getReaderName()};
    }
}
