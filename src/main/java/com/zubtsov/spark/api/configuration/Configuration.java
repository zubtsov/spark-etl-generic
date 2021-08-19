package com.zubtsov.spark.api.configuration;

//todo: is it general enough to assume that the configuration comprises only string values?
/**
 * The purpose of this class is to parametrize ETL job.
 * It can be useful to support environment-specific configuration or access any other external parameters.
 * Can be added as a parameter to any table reader, writer or builder.
 * @see com.zubtsov.spark.api.reading.TableReader
 * @see com.zubtsov.spark.api.writing.TableWriter
 * @see com.zubtsov.spark.api.Table
 */
public interface Configuration {
    /**
     * @param key - name of a configuration parameter
     * @return configuration parameter value
     */
    String get(String key);
}
