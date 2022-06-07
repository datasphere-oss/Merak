package io.merak.etl.executor.spark.metadata;

import io.merak.etl.sdk.metadata.*;
import io.merak.etl.utils.column.*;
import io.merak.etl.utils.config.*;
import io.merak.etl.executor.spark.translator.dto.*;

import org.apache.spark.sql.*;
import java.sql.*;
import io.merak.etl.executor.impl.*;
import com.google.common.collect.*;
import org.apache.spark.sql.types.*;
import scala.collection.mutable.*;
import java.util.*;

import io.merak.etl.pipeline.dto.*;
import org.slf4j.*;

public class SparkMetaDataUtils implements MetadataUtils
{
    static final Logger LOGGER;
    
    public static int getSqlType(final String typeName) {
        final String lowerCase = typeName.toLowerCase();
        switch (lowerCase) {
            case "string": {
                return 12;
            }
            case "decimal": {
                return 3;
            }
            case "timestamp": {
                return 93;
            }
            case "date": {
                return 91;
            }
            case "int": {
                return 4;
            }
            case "double": {
                return 8;
            }
            case "float": {
                return 6;
            }
            case "smallint": {
                return 5;
            }
            case "tinyint": {
                return -6;
            }
            case "boolean": {
                return 16;
            }
            case "binary": {
                return -2;
            }
            case "bigint": {
                return -5;
            }
            default: {
                SparkMetaDataUtils.LOGGER.warn("Unknown type {} ", (Object)typeName);
                return 12;
            }
        }
    }
    
    public static int getPrecision(final String typeName) {
        final String lowerCase = typeName.toLowerCase();
        switch (lowerCase) {
            case "string": {
                return Integer.MAX_VALUE;
            }
            case "decimal": {
                return 38;
            }
            case "timestamp": {
                return 29;
            }
            case "date": {
                return 24;
            }
            case "int": {
                return 23;
            }
            default: {
                SparkMetaDataUtils.LOGGER.warn("Unknown type {} ", (Object)typeName);
                return 0;
            }
        }
    }
    
    public static int getSize(final String typeName) {
        final String lowerCase = typeName.toLowerCase();
        switch (lowerCase) {
            case "string": {
                return Integer.MAX_VALUE;
            }
            case "decimal": {
                return 40;
            }
            case "timestamp": {
                return 29;
            }
            case "date": {
                return 24;
            }
            case "int": {
                return 23;
            }
            case "bigint": {
                return 0;
            }
            default: {
                SparkMetaDataUtils.LOGGER.error("Unknown type {} ", (Object)typeName);
                return 0;
            }
        }
    }
    
    public static int getScale(final String typeName) {
        final String lowerCase = typeName.toLowerCase();
        switch (lowerCase) {
            case "timestamp": {
                return 9;
            }
            default: {
                return 0;
            }
        }
    }
    
    public static List<TableColumn> getTableSchema(final SparkSession spark, final String schema, final String table, final boolean getHiddenColumns) throws Exception {
        final List<TableColumn> colNameTypes = (List<TableColumn>)Lists.newLinkedList();
        final String sourceQuery = String.format("select * from `%s`.`%s` limit 0", schema, table);
        final Dataset<Row> dataset = (Dataset<Row>)spark.sql(sourceQuery);
        final StructType my_schema = dataset.schema();
        final StructField[] fields2;
        final StructField[] fields = fields2 = my_schema.fields();
        for (final StructField col : fields2) {
            final String name = col.name();
            SparkMetaDataUtils.LOGGER.info("schema fetching column names is {}", (Object)col.name());
            final String type = col.dataType().simpleString().toLowerCase();
            SparkMetaDataUtils.LOGGER.info("schema fetching column type is {}", (Object)col.dataType().toString().toLowerCase());
            if (getHiddenColumns || !AwbUtil.isHiddenColumn(name)) {
                if (name == null || type == null || name.isEmpty()) {
                    break;
                }
                if (type.isEmpty()) {
                    break;
                }
                final TableColumn column = new TableColumn(name, type);
                colNameTypes.add(column);
            }
        }
        return colNameTypes;
    }
    
    public static List<TableColumn> getTableSchema(final Connection connection, final String schema, final String table) throws SQLException {
        final List<TableColumn> colNameTypes = (List<TableColumn>)Lists.newLinkedList();
        try (final Statement stmt = connection.createStatement()) {
            final String describeTable = String.format("DESCRIBE `%s`.`%s`", schema, table);
            SparkMetaDataUtils.LOGGER.debug("Getting schema for table {}.{}", (Object)schema, (Object)table);
            try (final ResultSet resultSet = stmt.executeQuery(describeTable)) {
                while (resultSet.next()) {
                    final String name = resultSet.getString(1);
                    final String type = resultSet.getString(2);
                    if (AwbUtil.isHiddenColumn(name)) {
                        continue;
                    }
                    if (name == null || type == null || name.isEmpty()) {
                        break;
                    }
                    if (type.isEmpty()) {
                        break;
                    }
                    colNameTypes.add(new TableColumn(name, type));
                }
            }
        }
        return colNameTypes;
    }
    
    public static void populate(final Row[] sampleData, final StructType schema, final ExecutionResult result) {
        populate(sampleData, schema, result, true);
    }
    
    public static void populate(final Row[] sampleData, final StructType schema, final ExecutionResult result, final boolean getData) {
        final Map<Integer, String> positionToIdMap = (Map<Integer, String>)Maps.newLinkedHashMap();
        final Map<Integer, String> positionToNameMap = (Map<Integer, String>)Maps.newLinkedHashMap();
        final Map<String, ExecutionResult.EntityResponse> outputEntities = (Map<String, ExecutionResult.EntityResponse>)Maps.newLinkedHashMap();
        for (int i = 0; i < schema.fields().length; ++i) {
            final StructField field = schema.apply(i);
            final String columnName = AwbUtil.removeTableNameFromColumnName(result.getTableName(), field.name());
            final Entity column = result.getViewingNode().getOutputEntityByName(columnName);
            if (column != null && !column.shouldExclude()) {
                String columnType = (field.dataType() == DataTypes.NullType) ? DataTypes.StringType.sql() : field.dataType().sql();
                final int precisionIndex = columnType.indexOf("(");
                if (precisionIndex != -1) {
                    columnType = columnType.substring(0, precisionIndex);
                }
                outputEntities.put(column.getId(), new ExecutionResult.EntityResponse(columnName, columnType.toLowerCase()));
                positionToIdMap.put(i, column.getId());
                positionToNameMap.put(i, columnName);
            }
        }
        long count = 0L;
        for (int j = 0; j < sampleData.length; ++j) {
            ++count;
            final Row row = sampleData[j];
            final Map<String, ExecutionResult.EntityResponse> data = (Map<String, ExecutionResult.EntityResponse>)Maps.newLinkedHashMap();
            for (final Map.Entry<Integer, String> entry : positionToIdMap.entrySet()) {
                final int index = entry.getKey();
                final Object value = row.get(index);
                if (value != null) {
                    if (value instanceof WrappedArray) {
                        outputEntities.get(entry.getValue()).addToData(row.getList(index).toString());
                    }
                    else {
                        outputEntities.get(entry.getValue()).addToData(value.toString());
                    }
                }
                else {
                    outputEntities.get(entry.getValue()).addToData("NULL");
                }
            }
        }
        result.setOutputEntities((Map)outputEntities);
        result.setCount(count);
    }
    
    public static void populateSchema(final List<TableColumn> schema, final ExecutionResult result) {
        final Map<String, ExecutionResult.EntityResponse> outputEntities = (Map<String, ExecutionResult.EntityResponse>)Maps.newLinkedHashMap();
        for (final TableColumn tc : schema) {
            final String columnName = AwbUtil.removeTableNameFromColumnName(result.getTableName(), tc.getName());
            final Entity column = result.getViewingNode().getOutputEntityByName(columnName);
            if (column != null && !column.shouldExclude()) {
                final String columnType = tc.getType();
                outputEntities.put(column.getId(), new ExecutionResult.EntityResponse(columnName, columnType.toLowerCase()));
            }
        }
        result.setOutputEntities((Map)outputEntities);
    }
    
    public static StructType getSchemaForTarget(final SparkTranslatorState sparkTranslatorState, final PipelineNode node) {
        return sparkTranslatorState.getDataFrame(node).getDataset().schema();
    }
    
    static {
        LOGGER = LoggerFactory.getLogger((Class)SparkMetaDataUtils.class);
    }
}
