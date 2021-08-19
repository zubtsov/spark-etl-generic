package com.zubtsov.spark.model.exception;

public class InvalidTableWriterException extends RuntimeException {
    public InvalidTableWriterException(Exception e) {
        super(e);
    }
}
