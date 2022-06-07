package io.merak.etl.executor.spark.utils;

import java.util.stream.*;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.expressions.*;
import org.spark_project.guava.base.*;
import scala.collection.*;
import java.util.*;
import org.slf4j.*;

public class SparkUtils
{
    private static final Logger LOGGER;
    
    public static Column toColumn(final String column) {
        return new Column(column);
    }
    
    public static List<Column> toColumns(final List<String> columns) {
        return columns.stream().map(i -> toColumn(i)).collect((Collector<? super Object, ?, List<Column>>)Collectors.toList());
    }
    
    public static Expression parseExpression(final SparkSession spark, final String expression) {
        try {
            return spark.sessionState().sqlParser().parseExpression(expression);
        }
        catch (Exception e) {
            Throwables.propagate((Throwable)e);
            return null;
        }
    }
    
    public static Column getColumnFromExp(final SparkSession spark, final String expression) {
        return new Column(parseExpression(spark, expression));
    }
    
    public static Seq<Column> getColumnSeqFromExpressions(final SparkSession spark, final List<String> expressions) {
        return ScalaUtils.toSeq(toColumnsFromExpression(spark, expressions));
    }
    
    public static List<Column> toColumnsFromExpression(final SparkSession spark, final List<String> expressions) {
        return expressions.stream().map(i -> getColumnFromExp(spark, i)).collect((Collector<? super Object, ?, List<Column>>)Collectors.toList());
    }
    
    public static Seq<String> getStringSeqFromList(final List<String> columns) {
        return ScalaUtils.toSeq(columns);
    }
    
    public static String getRandomUUID() {
        return UUID.randomUUID().toString();
    }
    
    static {
        LOGGER = LoggerFactory.getLogger((Class)SparkUtils.class);
    }
}
