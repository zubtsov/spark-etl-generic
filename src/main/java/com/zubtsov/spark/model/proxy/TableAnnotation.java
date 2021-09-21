package com.zubtsov.spark.model.proxy;

import com.zubtsov.spark.api.annotation.Table;
import org.apache.commons.lang3.StringUtils;

import java.lang.reflect.Method;
import java.lang.reflect.Parameter;

public class TableAnnotation {
    private final Table t;

    private TableAnnotation(Table t) {
        this.t = t;
    }

    public String getName() {
        return StringUtils.firstNonBlank(t.name(), t.value());
    }

    public String getReader() {
        return t.reader();
    }

    public static TableAnnotation ofMethod(Method method) {
        return new TableAnnotation(method.getAnnotation(Table.class));
    }

    public static TableAnnotation ofParameter(Parameter parameter) {
        return new TableAnnotation(parameter.getAnnotation(Table.class));
    }
}
