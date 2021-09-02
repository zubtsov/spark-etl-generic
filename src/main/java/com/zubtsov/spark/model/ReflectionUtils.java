package com.zubtsov.spark.model;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

//TODO: try to replace this class with some library
//TODO: we  heavily rely on the order of elements here...make it more robust
//FIXME: it's not guaranteed that the list is mutable
public class ReflectionUtils {
    public static Object invokeUnorderedArgs(Method m, Object obj, Object... args) throws InvocationTargetException, IllegalAccessException {
        //build map of queues for each argument type
        Map<String, List<Object>> classNameToArgument = Arrays.stream(args)
                .collect(Collectors.groupingBy(o -> {
                    if (o.getClass().isSynthetic()) {
                        return o.getClass().getInterfaces()[0].getCanonicalName();
                    } else {
                        return o.getClass().getCanonicalName();
                    }
                }));
        //go through parameters and take elements from the corresponding queue
        Object[] orderedArgs = Arrays
                .stream(m.getParameterTypes())
                .map(c -> classNameToArgument.get(c.getCanonicalName()).remove(0))
                .toArray();
        return m.invoke(obj, orderedArgs);
    }
}
