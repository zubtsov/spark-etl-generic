package com.zubtsov.spark.model;

import com.zubtsov.spark.model.proxy.Table;
import com.zubtsov.spark.model.proxy.TableReader;
import com.zubtsov.spark.model.proxy.TableWriter;
import org.apache.commons.collections4.iterators.IteratorIterable;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.jgrapht.Graph;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.DirectedAcyclicGraph;
import org.jgrapht.traverse.TopologicalOrderIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

//todo: add interface and implementation for parallel table building
public class TableDAG {
    private static final Logger LOGGER = LoggerFactory.getLogger(TableDAG.class);

    private final Map<String, TableReader> tableReaders;
    private final Map<String, TableWriter> tableWriters;
    private final Graph<Table, DefaultEdge> tableGraph;

    public TableDAG(Map<String, Table> nameToTable, Map<String, TableReader> tableReaders, Map<String, TableWriter> tableWriters) {
        this.tableReaders = tableReaders;
        this.tableWriters = tableWriters;
        this.tableGraph = getTableGraph(nameToTable);
    }

    public void buildTables() throws InvocationTargetException, InstantiationException, IllegalAccessException {
        Iterator<Table> topologicalOrderIterator = new TopologicalOrderIterator<>(tableGraph);
        Map<String, Dataset<Row>> builtTables = new HashMap<>();

        for (Table table : new IteratorIterable<>(topologicalOrderIterator)) {
            table.buildTable(builtTables, tableReaders, tableWriters);
            LOGGER.info(table.toString()); //todo: how to run jobs in parallel? perhaps, it's easier/better to use custom data structure rather than a graph
        }
    }
    //FIXME: external tables are not added to the graph, check if that's more flexible and convenient
    private static Graph<Table, DefaultEdge> getTableGraph(final Map<String, Table> tableNameToTable) {
        Graph<Table, DefaultEdge> g = new DirectedAcyclicGraph<>(DefaultEdge.class);
        tableNameToTable.values().stream().forEach(g::addVertex);

        tableNameToTable.forEach((currentTableName, table) -> {
            table.getDependencyTablesNames()
                    .forEach(dependencyTableName -> {
                        if (tableNameToTable.containsKey(dependencyTableName)) {
                            LOGGER.info("Internal table '{}' depends on internal table '{}'", currentTableName, dependencyTableName);
                            g.addEdge(tableNameToTable.get(dependencyTableName), table);
                        } else {
                            LOGGER.info("Internal table '{}' depends on external table '{}'", currentTableName, dependencyTableName);
                        }
                    });
        });

        return g;
    }
}
