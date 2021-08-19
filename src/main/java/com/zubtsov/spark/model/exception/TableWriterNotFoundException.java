package com.zubtsov.spark.model.exception;

public class TableWriterNotFoundException extends RuntimeException {
    public TableWriterNotFoundException(String tableWriterName) {
        super("Table writer " + tableWriterName + " not found");
    }
}
