package com.zubtsov.spark.api.annotation.writing;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

//TODO: think about other names like TableStorage, TableRepository
/**
 * The declaring class must have no-arguments constructor.
 * The method must accept table name argument of String type, table data of org.apache.spark.sql.DataFrame type and return written table of org.apache.spark.sql.DataFrame type.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD})
public @interface TableWriter {
    String name();
}
