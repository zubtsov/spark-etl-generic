package com.zubtsov.spark.api.annotation.writing;

import java.lang.annotation.*;

import static com.zubtsov.spark.api.annotation.writing.TableWriter.DEFAULT_WRITER_NAME;
//todo: support write options? is there any options that should be associated with the table or all of them should
// be configured in table writer? perhaps it's 50/50
/**
 * It's an indicator, that a table should be saved.
 * Storage format should not be specified here, it's not a TableBuilder's concern.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD})
@Repeatable(MultipleSave.class)
public @interface Save {
    /**
     * Table writer name
     * @see TableWriter
     */
    String writer() default DEFAULT_WRITER_NAME; //todo: think about other binding strategies different from "by name"
}
