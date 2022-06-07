package io.merak.etl.executor.spark.session;

import org.apache.spark.sql.*;

@FunctionalInterface
public interface DatasetProcessor
{
    void process(final Dataset<Row> p0);
}
