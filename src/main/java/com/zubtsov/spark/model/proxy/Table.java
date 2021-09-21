package com.zubtsov.spark.model.proxy;

import com.zubtsov.spark.api.annotation.configuration.Configuration;
import com.zubtsov.spark.api.annotation.writing.Save;
import com.zubtsov.spark.model.ReflectionUtils;
import com.zubtsov.spark.model.exception.TableReaderNotFoundException;
import com.zubtsov.spark.model.exception.TableWriterNotFoundException;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.Validate;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

//TODO: try to avoid getters as they violate encapsulation
public final class Table {
    private final String tableName;
    private final Method buildTableMethod;
    private final List<String> tableWritersNames;
    private final Map<String, String> dependencyTablesToReaders;

    private Table(final Method btm) {
        validateBuildMethod(btm);
        this.buildTableMethod = btm;
        this.tableName = TableAnnotation.ofMethod(btm).getName();
        validateTableName(tableName);
        this.dependencyTablesToReaders = getDependencies(btm);
        this.tableWritersNames = Arrays.stream(btm.getAnnotationsByType(Save.class)).map(Save::writer).collect(Collectors.toList());
        Collections.reverse(tableWritersNames);
    }

    public Dataset<Row> processTable(Map<String, Dataset<Row>> builtTables, Map<String, TableReader> tableReaders, Map<String, TableWriter> tableWriters, Configuration configuration) throws InstantiationException, IllegalAccessException, InvocationTargetException {
        Dataset[] dependencies = getDatasetDependencies(builtTables, tableReaders, configuration);
        List<Object> args = new ArrayList<>(Arrays.asList(dependencies));
        args.add(configuration);
        Object builderObject = buildTableMethod.getDeclaringClass().newInstance();
        Dataset<Row> builtTable = (Dataset<Row>) ReflectionUtils.invokeUnorderedArgs(buildTableMethod, builderObject, args.toArray());

        Dataset<Row> lastWrittenTable = null;
        for (String trn : tableWritersNames) {
            TableWriter tableWriter = tableWriters.get(trn);
            if (tableWriter == null) {
                throw new TableWriterNotFoundException(trn);
            }
            lastWrittenTable = tableWriter.write(tableName, builtTable, configuration);
        }

        Dataset<Row> resultTable = ObjectUtils.firstNonNull(lastWrittenTable, builtTable);
        builtTables.put(tableName, resultTable);
        return resultTable;
    }

    private Dataset[] getDatasetDependencies(Map<String, Dataset<Row>> builtTables, Map<String, TableReader> tableReaders, Configuration configuration) {
        return dependencyTablesToReaders.entrySet().stream().map(entry -> {
            String tn = entry.getKey();
            String dependencyTableReader = entry.getValue();
            if (builtTables.containsKey(tn)) {
                return builtTables.get(tn);
            } else {
                try {
                    TableReader r = tableReaders.get(dependencyTableReader);
                    if (r == null) {
                        throw new TableReaderNotFoundException(dependencyTableReader);
                    }
                    return r.read(tn, configuration);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }).toArray(Dataset[]::new);
    }

    public static Table ofMethod(Method buildTable) {
        return new Table(buildTable);
    }

    @Override
    public int hashCode() {
        return tableName.hashCode();
    }

    @Override
    public boolean equals(final Object obj) {
        return EqualsBuilder.reflectionEquals(this, obj, "buildMethod");
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this);
    }

    public Set<String> getDependencyTablesNames() {
        return dependencyTablesToReaders.keySet();
    }

    private static Map<String, String> getDependencies(final Method buildTable) {
        Parameter[] pa = buildTable.getParameters();
        Annotation[][] pas = buildTable.getParameterAnnotations();

        return IntStream.range(0, pas.length)
                .filter(i -> {
                    Annotation[] parameterAnnotations = pas[i];
                    return Arrays.stream(parameterAnnotations)
                            .anyMatch(a -> Objects.equals(a.annotationType(), com.zubtsov.spark.api.annotation.Table.class));
                })
                .boxed()
                .collect(Collectors.toMap(
                        i -> TableAnnotation.ofParameter(pa[i]).getName(),
                        i -> TableAnnotation.ofParameter(pa[i]).getReader(),
                        (x, y) -> y,
                        LinkedHashMap::new
                ));
    }

    private static void validateBuildMethod(Method buildMethod) {
        if (buildMethod.getReturnType() != Dataset.class) {
            throw new IllegalArgumentException("The return type of a build method should be org.apache.spark.sql.DataFrame");
        }
        //todo: check that the method doesn't accept arguments which framework is not able to pass
    }

    private static void validateTableName(final String tableName) {
        Validate.notNull(tableName);
        Validate.notEmpty(tableName.trim());
        //todo: there might be limitations from the storage side, check them depending on where the table is stored?
    }
}
