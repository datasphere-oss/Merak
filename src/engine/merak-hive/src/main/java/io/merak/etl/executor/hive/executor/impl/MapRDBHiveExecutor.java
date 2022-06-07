package io.merak.etl.executor.hive.executor.impl;

import org.slf4j.*;

import java.io.*;

import com.google.common.collect.*;

import merak.tools.ExceptionHandling.*;
import io.merak.etl.context.*;
import io.merak.etl.executor.hive.task.*;
import io.merak.etl.executor.hive.translator.*;
import io.merak.etl.executor.impl.*;
import io.merak.etl.pipeline.dto.*;
import io.merak.etl.pipeline.dto.convertors.*;
import io.merak.etl.sdk.engine.*;
import io.merak.etl.sdk.exceptions.*;
import io.merak.etl.sdk.session.*;
import io.merak.etl.sdk.task.*;
import io.merak.etl.session.*;
import io.merak.etl.task.impl.*;
import io.merak.etl.utils.config.*;
import io.merak.metadata.impl.dto.tables.columns.*;

import java.util.*;
import java.util.function.*;
import java.sql.*;
import com.google.common.base.*;

public class MapRDBHiveExecutor extends LoadTargetExecutor
{
    protected Logger LOGGER;
    JdbcExecutionSession jdbcExecutionSession;
    Map<String, String> schema;
    MapRDBHiveTranslator batchTargetTranslator;
    
    public MapRDBHiveExecutor() {
        this.LOGGER = LoggerFactory.getLogger((Class)this.getClass());
    }
    
    protected void init(final Task task, final RequestContext requestContext) {
        this.reqCtx = requestContext;
        this.task = (LoadTargetTaskNode)task;
        this.targetNode = ((KeyStoreTargetTaskNode)task).getSink();
        this.jdbcExecutionSession = (JdbcExecutionSession)ExecutionEngineSessionFactory.getExecutionSession(this.reqCtx);
        this.batchTargetTranslator = (MapRDBHiveTranslator)((KeyStoreTargetTaskNode)task).getSinkTranslator();
        this.schema = this.getSchema();
    }
    
    protected void overWrite(final LoadTarget targetNode) throws IOException, ExecutionSessionException {
        final List<String> tableStmts = this.getPreOverWriteStmt(targetNode);
        final String schemaName = ((MapRDBKeyStoreTarget)targetNode).getImposedSchemaName();
        final String tableName = ((MapRDBKeyStoreTarget)targetNode).getImposedTableName();
        final String insertstmt = this.batchTargetTranslator.getInsertStmt(schemaName, tableName);
        tableStmts.add(insertstmt);
        try {
            this.jdbcExecutionSession.executeStatements((List)tableStmts);
        }
        catch (Exception e) {
            this.LOGGER.error("Exception: {}", (Object)e.getMessage());
            Throwables.propagate((Throwable)e);
        }
    }
    
    protected void merge(final LoadTarget targetNode) {
        final List<String> insertStmts = (List<String>)Lists.newArrayList();
        final String schemaName = ((MapRDBKeyStoreTarget)targetNode).getImposedSchemaName();
        final String tableName = ((MapRDBKeyStoreTarget)targetNode).getImposedTableName();
        final String insertstmt = this.batchTargetTranslator.getInsertStmt(schemaName, tableName);
        this.LOGGER.info("merge function for table {}", (Object)tableName);
        insertStmts.add(insertstmt);
        try {
            this.jdbcExecutionSession.executeStatements((List)insertStmts);
        }
        catch (Exception e) {
            this.LOGGER.error("Exception: {}", (Object)e.getMessage());
            Throwables.propagate((Throwable)e);
        }
    }
    
    protected void append(final LoadTarget targetNode) {
        final RuntimeException runtimeException = new RuntimeException(String.format("Unknown syncmode %s !! exiting", targetNode.getSyncMode()));
        Throwables.propagate((Throwable)runtimeException);
    }
    
    protected boolean shouldUpdateMetaStore() {
        return true;
    }
    
    protected List<String> getPreOverWriteStmt(final LoadTarget targetNode) throws ExecutionSessionException, IOException {
        this.LOGGER.info("schema size is {}", (Object)this.schema.keySet().size());
        final List<String> tableStmts = this.batchTargetTranslator.getTableCreationForOverWrite(this.schema, ((KeyValueTargetNode)targetNode).getImposedTableName());
        return tableStmts;
    }
    
    protected Map<String, Long> getModifiedRecord() {
        final Map<String, Long> modifiedRecords = (Map<String, Long>)Maps.newHashMap();
        final Date jobStartTime = this.reqCtx.getProcessingContext().getJobStartTime();
        final String targetSchemaName = this.targetNode.getSchemaName();
        modifiedRecords.put("RowsInserted", 0L);
        modifiedRecords.put("RowsUpdated", 0L);
        modifiedRecords.put("RowsDeleted", 0L);
        if (this.targetNode.getSyncMode() == LoadTarget.SyncMode.OVERWRITE) {
            return modifiedRecords;
        }
        try {
            final String getStmt = String.format("SELECT `%s`, COUNT(*) `record_count` FROM `%s`.`%s` WHERE `%s` >= CAST (%d AS TIMESTAMP) GROUP BY `%s`", "ZIW_STATUS_FLAG".toLowerCase(), targetSchemaName, this.targetNode.getTableName(), "ZIW_UPDATED_TIMESTAMP".toLowerCase(), jobStartTime.getTime(), "ZIW_STATUS_FLAG".toLowerCase());
            this.jdbcExecutionSession.executeQuery(getStmt, resultSet -> {
                if (resultSet != null) {
                    while (resultSet.next()) {
                        final String operation = resultSet.getString(1);
                        final Long rowcount = resultSet.getLong(2);
                        final String upperCase = operation.toUpperCase();
                        switch (upperCase) {
                            case "I": {
                                modifiedRecords.put("RowsInserted", rowcount);
                                continue;
                            }
                            case "U": {
                                modifiedRecords.put("RowsUpdated", rowcount);
                                continue;
                            }
                            case "D": {
                                modifiedRecords.put("RowsDeleted", rowcount);
                                continue;
                            }
                        }
                    }
                }
            });
        }
        catch (SQLException e) {
            this.LOGGER.error("Exception {}", (Object)AwbUtil.getErrorMessage((Throwable)e));
            final IWRunTimeException runTimeException = new IWRunTimeException("{GET_MODIFIED_RECORDS_ERROR}", (Throwable)e);
            Throwables.propagate((Throwable)runTimeException);
        }
        return modifiedRecords;
    }
    
    protected MetaStoreObjectGenerator getMetastoreObjectConverter(final LoadTarget targetNode, final RequestContext requestContext, final List<ColumnProperties> tableColumns) {
        return (MetaStoreObjectGenerator)new KeyValueMetastoreObjectConverter(targetNode, requestContext, (List)tableColumns);
    }
    
    protected boolean isEligibleForMergeOptimization() {
        return false;
    }
    
    protected SupportedExecutionEngines getSupportedEngines() {
        return new SupportedExecutionEngines((List)Arrays.asList(BatchEngine.HIVE), (List)Arrays.asList(this.reqCtx.getBatchEngine()));
    }
    
    protected List<ColumnProperties> getTargetSchema() {
        final String getSchemaStmt = String.format("SELECT * FROM `%s`.`%s` LIMIT 0", this.targetNode.getSchemaName(), this.targetNode.getTableName());
        final List<ColumnProperties> columnList = new ArrayList<ColumnProperties>();
        try {
            this.LOGGER.debug("Trying to get Result-Set MetaData using statement: {}", (Object)getSchemaStmt);
            this.jdbcExecutionSession.executeQuery(getSchemaStmt, resultSet -> {
                Preconditions.checkNotNull((Object)resultSet, (Object)"ResultSet is null");
                final ResultSetMetaData meta = resultSet.getMetaData();
                this.LOGGER.debug("ReultSet MetaData has {} columns. Populating columnList:", (Object)meta.getColumnCount());
                this.populateColumnList(this.targetNode.getTableName(), meta, columnList);
            });
        }
        catch (SQLException ex) {
            this.LOGGER.error("Exception {}", (Object)AwbUtil.getErrorMessage((Throwable)ex));
            Throwables.propagate((Throwable)new IWRunTimeException("{HIVE_TABLE_METADATA_READ_ERROR}", (Throwable)ex));
        }
        return columnList;
    }
    
    private void populateColumnList(final String targetTableName, final ResultSetMetaData resultSetMeta, final List<ColumnProperties> columnList) {
        try {
            int i = 1;
            int pos = 1;
            while (i <= resultSetMeta.getColumnCount()) {
                final String origColumnName = AwbUtil.removeTableNameFromColumnName(targetTableName.toLowerCase(), resultSetMeta.getColumnName(i));
                if (!AwbUtil.isHiddenColumn(origColumnName)) {
                    final String colType = resultSetMeta.getColumnTypeName(i).toLowerCase();
                    final int sqlType = resultSetMeta.getColumnType(i);
                    final int precision = resultSetMeta.getPrecision(i);
                    final int scale = resultSetMeta.getScale(i);
                    final int size = resultSetMeta.getColumnDisplaySize(i);
                    final ColumnProperties colObj = new ColumnProperties();
                    colObj.setName(origColumnName);
                    colObj.setAt(Integer.valueOf(pos++));
                    colObj.setType(colType);
                    colObj.setSqlType(Integer.valueOf(sqlType));
                    colObj.setPrecision(Integer.valueOf(precision));
                    colObj.setRole("o");
                    colObj.setSize(Integer.valueOf(size));
                    colObj.setAuditColumn(Boolean.valueOf(AwbUtil.isAuditColumn(origColumnName)));
                    columnList.add(colObj);
                }
                ++i;
            }
        }
        catch (Exception ex) {
            this.LOGGER.error("Error while getting column list for target");
            Throwables.propagate((Throwable)ex);
        }
        this.LOGGER.debug("List of Columns to be populated to MetaStore for Target table {} are: {}", (Object)targetTableName, (Object)columnList.toString());
    }
    
    protected long getTargetRowCount() {
        long rowCount = 0L;
        final StringBuilder numRowsStr = new StringBuilder("0");
        final String stmt = String.format("SELECT COUNT(*) AS `ROW_COUNT` FROM `%s`.`%s`", this.targetNode.getSchemaName(), this.targetNode.getTableName());
        try {
            this.jdbcExecutionSession.executeQuery(stmt, resultSet -> {
                if (resultSet != null) {
                    while (resultSet.next()) {
                        numRowsStr.append(resultSet.getString(1));
                    }
                }
            });
        }
        catch (Exception e) {
            this.LOGGER.error("Exception {}", (Object)AwbUtil.getErrorMessage((Throwable)e));
            Throwables.propagate((Throwable)new IWRunTimeException("{HIVE_TABLE_METADATA_READ_ERROR}", (Throwable)e));
        }
        rowCount = Long.parseLong(numRowsStr.toString());
        this.LOGGER.debug("Row count calculated for target table {}", (Object)rowCount);
        return rowCount;
    }
    
    protected Map<String, String> getSchema() {
        final String schemaName = ((MapRDBKeyStoreTarget)this.targetNode).getImposedSchemaName();
        final String dummyTableName = "iw_schema_extract_" + ((MapRDBKeyStoreTarget)this.targetNode).getImposedTableName();
        String newType;
        final Function<String, String> typeOverWrite = (Function<String, String>)(type -> {
            newType = type;
            if (type.equals("void")) {
                newType = "string";
            }
            else if (type.contains("decimal")) {
                newType = "double";
            }
            return newType;
        });
        final Map<String, String> colNameTypes = this.batchTargetTranslator.getSchema(this.reqCtx, schemaName, dummyTableName, typeOverWrite);
        return colNameTypes;
    }
}
