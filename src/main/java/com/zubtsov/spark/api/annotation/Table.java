package com.zubtsov.spark.api.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import static com.zubtsov.spark.common.DefaultNames.DEFAULT_READER_NAME;

@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD, ElementType.PARAMETER})
/**
 *  The declaring class must have no-arguments constructor.
 *  When applied to a method denotes output table, denotes input table when applied to a method parameter.
 *  Self references only allowed when the table is persisted somewhere.
 *  There are 2 types of tables: internal - produced by the pipeline and external - exist independently of the pipeline.
 */
public @interface Table {
    /**
     * A synonym of the name. name() has higher priority than value().
     * @return table name
     * @see Table#name()
     */
    String value() default "";
    /**
     * Table name. Avoid using it for purposes other than uniquely identifying a table.
     * For example if you rely on some prefix in table name to decide where to write a table,
     * you'll have to remove it before writing to a database/storage. Don't do it, please.
     * @return table name
     */
    String name() default "";

    /**
     * Table reader name
     * @see com.zubtsov.spark.api.annotation.reading.TableReader
     */
    String reader() default DEFAULT_READER_NAME;
}
