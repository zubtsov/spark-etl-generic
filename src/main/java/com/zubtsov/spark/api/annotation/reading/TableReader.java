package com.zubtsov.spark.api.annotation.reading;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

//TODO: think about other names like ExternalTableReader, TableSupplier, TableProvider, TableRepository, TableStorage, Storage or something else
/**
 * The declaring class must have no-arguments constructor.
 * The method must accept table name argument of String type and return read table of org.apache.spark.sql.DataFrame object type.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD})
public @interface TableReader {
    String name();
}
