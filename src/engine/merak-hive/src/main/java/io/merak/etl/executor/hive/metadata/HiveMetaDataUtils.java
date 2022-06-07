package io.merak.etl.executor.hive.metadata;

import com.google.common.base.*;
import java.util.*;

import com.google.common.collect.*;

import io.merak.etl.executor.impl.*;
import io.merak.etl.pipeline.dto.*;
import io.merak.etl.sdk.metadata.*;
import io.merak.etl.utils.column.*;
import io.merak.etl.utils.config.*;

import java.sql.*;
import org.slf4j.*;

public class HiveMetaDataUtils implements MetadataUtils
{
    static final Logger LOGGER;
    
    public static void populate(final ResultSet sampleData, final ExecutionResult result) throws SQLException {
        populate(sampleData, result, false);
    }
    
    public static void populate(final ResultSet sampleData, final ExecutionResult result, final boolean getData) throws SQLException {
        final Map<Integer, String> positionToIdMap = (Map<Integer, String>)Maps.newLinkedHashMap();
        final Map<String, ExecutionResult.EntityResponse> outputEntities = (Map<String, ExecutionResult.EntityResponse>)Maps.newHashMap();
        final ResultSetMetaData metaData = sampleData.getMetaData();
        final int nColumns = metaData.getColumnCount();
        final String tableName = result.getTableName();
        for (int i = 1; i <= nColumns; ++i) {
            final String name = AwbUtil.removeTableNameFromColumnName(tableName, metaData.getColumnName(i));
            Column column = (Column)result.getViewingNode().getOutputEntityByName(name);
            if (column == null) {
                final String new_name = String.format("_%s", name);
                column = (Column)result.getViewingNode().getOutputEntityByName(new_name);
            }
            if (!AwbUtil.isHiddenColumn(name)) {
                Preconditions.checkState(column != null, "Column %s not found!", new Object[] { name });
                int sqlType = metaData.getColumnType(i);
                String columnTypeName = metaData.getColumnTypeName(i).toLowerCase();
                if (sqlType == 0) {
                    result.getViewingNode().setVoidEntityWarning(i - 1);
                    result.setHasVoidColumns(true);
                    sqlType = 12;
                    columnTypeName = "string";
                }
                outputEntities.put(column.getId(), new ExecutionResult.EntityResponse(name, columnTypeName));
                if (getData) {
                    positionToIdMap.put(i, column.getId());
                }
            }
        }
        long count = 0L;
        if (getData) {
            while (sampleData.next()) {
                ++count;
                for (final Map.Entry<Integer, String> entry : positionToIdMap.entrySet()) {
                    final int j = entry.getKey();
                    String value = null;
                    switch (metaData.getColumnType(j)) {
                        case 2005: {
                            final Clob clob = sampleData.getClob(j);
                            value = ((clob == null) ? null : clob.getSubString(1L, (int)clob.length()));
                            break;
                        }
                        case -2:
                        case 2004: {
                            value = ((sampleData.getObject(j) == null) ? null : "<binary>");
                            break;
                        }
                        default: {
                            final String data = sampleData.getString(j);
                            value = ((data == null) ? "NULL" : data);
                            break;
                        }
                    }
                    outputEntities.get(entry.getValue()).addToData(value);
                }
            }
        }
        result.setOutputEntities((Map)outputEntities);
        result.setCount(count);
    }
    
    public static List<TableColumn> getTableSchema(final Connection connection, final String schema, final String table, final boolean getHiddenColumns) throws SQLException {
        final List<TableColumn> colNameTypes = (List<TableColumn>)Lists.newLinkedList();
        try (final Statement stmt = connection.createStatement()) {
            final String describeTable = String.format("DESCRIBE `%s`.`%s`", schema, table);
            HiveMetaDataUtils.LOGGER.debug("Getting schema for table {}.{}", (Object)schema, (Object)table);
            try (final ResultSet resultSet = stmt.executeQuery(describeTable)) {
                while (resultSet.next()) {
                    final String name = resultSet.getString(1);
                    final String type = resultSet.getString(2);
                    if (!getHiddenColumns && AwbUtil.isHiddenColumn(name)) {
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
    
    public static void populateSchema(final List<TableColumn> outputColumns, final ExecutionResult result) throws SQLException {
        final Map<String, ExecutionResult.EntityResponse> outputEntities = (Map<String, ExecutionResult.EntityResponse>)Maps.newHashMap();
        final int nOutputColumns = outputColumns.size();
        final String tableName = result.getTableName();
        for (int i = 0; i < nOutputColumns; ++i) {
            final String name = AwbUtil.removeTableNameFromColumnName(tableName, outputColumns.get(i).getName());
            Column column = (Column)result.getViewingNode().getOutputEntityByName(name);
            if (column == null) {
                final String new_name = String.format("_%s", name);
                column = (Column)result.getViewingNode().getOutputEntityByName(new_name);
            }
            if (!AwbUtil.isHiddenColumn(name)) {
                Preconditions.checkState(column != null, "Column %s not found!", new Object[] { name });
                final String columnTypeName = outputColumns.get(i).getType().toLowerCase();
                outputEntities.put(column.getId(), new ExecutionResult.EntityResponse(name, columnTypeName));
            }
        }
        result.setOutputEntities((Map)outputEntities);
    }
    
    static {
        LOGGER = LoggerFactory.getLogger((Class)HiveMetaDataUtils.class);
    }
}
