package com.zubtsov.spark.api.extension.configuration;

/**
 * The purpose of this class is to parametrize ETL job.
 * It can be useful to support environment-specific configuration or access any other external parameters.
 * Can be accessed by any table reader, writer or builder.
 */
public interface Configuration<T> {
    /**
     * @param key - name of a configuration parameter
     * @return configuration parameter value
     */
    T get(String key);
}
