package com.zubtsov.spark.model.exception;

public class InvalidTableReaderException extends RuntimeException {
    public InvalidTableReaderException(Exception e) {
        super(e);
    }
}
