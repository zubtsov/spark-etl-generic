package com.zubtsov.spark.model.proxy;

import com.zubtsov.spark.model.exception.InvalidTableWriterException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.lang.reflect.Method;

public class TableWriter {
    private final String name;
    private final Object writerObject;
    private final Method writeMethod;

    private TableWriter(Method writeMethod) {
        this.name =  writeMethod.getAnnotation(com.zubtsov.spark.api.writing.TableWriter.class).name();
        this.writeMethod = writeMethod;
        try {
            this.writerObject = writeMethod.getDeclaringClass().newInstance();
        } catch (Exception e) {
            throw new InvalidTableWriterException(e);
        }
    }

    public Dataset<Row> write(String tableName, Dataset<Row> tableData) {
        try {
            if (writeMethod.getParameters()[0].getType() == String.class) {
                return (Dataset<Row>) writeMethod.invoke(writerObject, tableName, tableData);
            } else {
                return (Dataset<Row>) writeMethod.invoke(writerObject, tableData, tableName);
            }
        } catch (Exception e) {
            throw new InvalidTableWriterException(e);
        }
    }

    public String getName() {
        return name;
    }

    public static TableWriter ofMethod(Method writeMethod) {
        return new TableWriter(writeMethod);
    }
}
