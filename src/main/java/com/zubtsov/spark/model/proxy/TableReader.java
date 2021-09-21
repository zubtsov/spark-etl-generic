package com.zubtsov.spark.model.proxy;

import com.zubtsov.spark.api.annotation.configuration.Configuration;
import com.zubtsov.spark.model.ReflectionUtils;
import com.zubtsov.spark.model.exception.InvalidTableReaderException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.lang.reflect.Method;

public class TableReader {
    private final String name;
    private final Object readerObject;
    private final Method readMethod;

    private TableReader(String name, Method readMethod) {
        this.name = name;
        try {
            this.readerObject = readMethod.getDeclaringClass().newInstance();
        } catch (Exception e) {
            throw new InvalidTableReaderException(e);
        }
        this.readMethod = readMethod;
    }

    public Dataset<Row> read(String tableName, Configuration configuration) {
        try {
            return (Dataset<Row>) ReflectionUtils.invokeUnorderedArgs(readMethod, readerObject,
                    tableName, configuration);
        } catch (Exception e) {
            throw new InvalidTableReaderException(e);
        }
    }

    public String getName() {
        return name;
    }

    public static TableReader ofMethod(Method readMethod) {
        String name = readMethod.getAnnotation(com.zubtsov.spark.api.annotation.reading.TableReader.class).name();
        return new TableReader(name, readMethod);
    }
}
