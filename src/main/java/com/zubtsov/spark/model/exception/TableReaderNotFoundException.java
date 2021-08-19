package com.zubtsov.spark.model.exception;

public class TableReaderNotFoundException extends RuntimeException {
    public TableReaderNotFoundException(String tableReaderName) {
        super("Table reader " + tableReaderName + " not found");
    }
}
