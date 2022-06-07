package io.merak.etl.executor.spark.translator.dto;

import org.apache.spark.sql.*;

import io.merak.etl.executor.spark.translator.*;

import java.util.*;

public class DataFrameObject
{
    Dataset<Row> dataset;
    Map<String, String> renames;
    String sparkExecutionSteps;
    
    public DataFrameObject(final Dataset<Row> dataset, final String sparkExecutionSteps) {
        this.dataset = dataset;
        this.sparkExecutionSteps = sparkExecutionSteps;
        new SparkExecutionSteps(sparkExecutionSteps);
    }
    
    public DataFrameObject(final Dataset<Row> dataset, final String sparkExecutionSteps, final Map<String, String> renames) {
        this.dataset = dataset;
        this.renames = renames;
        this.sparkExecutionSteps = sparkExecutionSteps;
        new SparkExecutionSteps(sparkExecutionSteps);
    }
    
    public List<String> getLogicalPlan() {
        final List<String> list = new ArrayList<String>();
        list.add(this.sparkExecutionSteps);
        list.add(String.format("Columns are : %s", Arrays.toString(this.dataset.columns())));
        if (this.renames != null) {
            final StringBuilder builder = new StringBuilder();
            this.renames.forEach((k, v) -> builder.append(String.format("%s -> %s |", k, v)));
            list.add(builder.toString());
        }
        return list;
    }
    
    public Dataset<Row> getDataset() {
        return this.dataset;
    }
}
