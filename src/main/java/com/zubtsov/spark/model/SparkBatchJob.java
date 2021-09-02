package com.zubtsov.spark.model;

import com.zubtsov.spark.api.configuration.Configuration;
import com.zubtsov.spark.api.reading.TableReader;
import com.zubtsov.spark.api.writing.TableWriter;
import com.zubtsov.spark.model.proxy.Table;
import com.zubtsov.spark.model.proxy.TableAnnotation;
import org.reflections.Reflections;
import org.reflections.scanners.MethodAnnotationsScanner;
import org.reflections.scanners.SubTypesScanner;

import java.lang.reflect.InvocationTargetException;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

//todo: support streaming case?
//todo: add entry point for the job in the API package
public class SparkBatchJob {
    private final Reflections reflections;
    private final Map<String, Table> nameToTable;
    private final Map<String, com.zubtsov.spark.model.proxy.TableReader> tableReaders;
    private final Map<String, com.zubtsov.spark.model.proxy.TableWriter> tableWriters;

    public SparkBatchJob(final String packageName) {
        this.reflections = new Reflections(packageName, new SubTypesScanner(false), new MethodAnnotationsScanner());
        this.tableReaders = collectReaders();
        this.tableWriters = collectWriters();
        this.nameToTable = collectTables();
    }

    public void run(Configuration configuration) throws InvocationTargetException, InstantiationException, IllegalAccessException {
        new TableDAG(nameToTable, tableReaders, tableWriters, configuration).buildTables();
    }

    private Map<String, Table> collectTables() {
        return reflections.getMethodsAnnotatedWith(com.zubtsov.spark.api.Table.class).stream().collect(Collectors.toMap(
                m -> TableAnnotation.ofMethod(m).getName(),
                Function.identity()
        )).entrySet().stream().collect(Collectors.toMap(
                Map.Entry::getKey,
                entry -> Table.ofMethod(entry.getValue())
        ));
    }

    private Map<String, com.zubtsov.spark.model.proxy.TableWriter> collectWriters() {
        return reflections.getMethodsAnnotatedWith(TableWriter.class).stream().map(com.zubtsov.spark.model.proxy.TableWriter::ofMethod)
                .collect(Collectors.toMap(
                        com.zubtsov.spark.model.proxy.TableWriter::getName,
                        Function.identity()
                ));
    }

    private Map<String, com.zubtsov.spark.model.proxy.TableReader> collectReaders() {
        return reflections.getMethodsAnnotatedWith(TableReader.class).stream()
                .map(com.zubtsov.spark.model.proxy.TableReader::ofMethod)
                .collect(Collectors.toMap(
                        com.zubtsov.spark.model.proxy.TableReader::getName,
                        Function.identity()
                ));
    }
}
