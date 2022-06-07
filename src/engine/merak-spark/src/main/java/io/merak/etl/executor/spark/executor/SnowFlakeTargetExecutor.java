package io.merak.etl.executor.spark.executor;

import io.merak.etl.executor.impl.*;

import java.util.concurrent.atomic.*;

import io.merak.etl.sdk.task.*;
import io.merak.etl.context.*;
import java.util.stream.*;
import merak.tools.ExceptionHandling.*;
import com.google.common.base.*;
import com.google.common.collect.*;
import org.apache.spark.sql.types.*;
import io.merak.etl.pipeline.dto.*;
import io.merak.metadata.impl.dto.tables.columns.*;
import io.merak.etl.executor.spark.exceptions.*;
import io.merak.etl.executor.spark.metadata.*;
import io.merak.etl.executor.spark.session.*;
import io.merak.etl.executor.spark.task.*;
import io.merak.etl.executor.spark.translator.*;
import io.merak.etl.executor.spark.translator.dto.*;

import java.util.*;

import org.apache.spark.sql.*;
import io.merak.etl.pipeline.dto.convertors.*;
import io.merak.etl.utils.config.*;
import java.sql.*;
import org.slf4j.*;

public class SnowFlakeTargetExecutor extends LoadTargetExecutor
{
    private static final Logger LOGGER;
    protected SnowFlakeTargetTranslator translator;
    protected SparkSession spark;
    protected SparkTranslatorState sparkTranslatorState;
    protected SparkSnowFlakeTaskNode taskNode;
    protected AtomicLong insertedRecords;
    protected AtomicLong updatedRecords;
    protected AtomicLong deletedRecords;
    protected SnowflakeSession snowflakeSession;
    protected long rowCount;
    private SnowFlakeTargetNode snowFlakeTargetNode;
    
    public SnowFlakeTargetExecutor() {
        this.spark = null;
    }
    
    protected void init(final Task task, final RequestContext requestContext) {
        this.reqCtx = requestContext;
        this.taskNode = (SparkSnowFlakeTaskNode)task;
        this.targetNode = this.taskNode.getSink();
        this.snowFlakeTargetNode = (SnowFlakeTargetNode)this.targetNode;
        this.translator = (SnowFlakeTargetTranslator)((SparkSnowFlakeTaskNode)task).getSinkTranslator();
        this.spark = this.translator.getSpark();
        this.snowflakeSession = new SnowflakeSession(this.reqCtx, this.snowFlakeTargetNode);
        (this.sparkTranslatorState = (SparkTranslatorState)this.taskNode.getTranslatorState()).runTranslations();
        this.insertedRecords = new AtomicLong(0L);
        this.updatedRecords = new AtomicLong(0L);
        this.deletedRecords = new AtomicLong(0L);
    }
    
    private Dataset executeStmt(final SparkSession sparkSession, final SnowFlakeTargetNode node, final String sql) {
        final DataFrameReader read = sparkSession.read();
        node.getClass();
        return read.format("net.snowflake.spark.snowflake").options((Map)node.getSnowFlakeConnectionProp()).option("query", sql).load();
    }
    
    private Dataset getTableSchema(final SparkSession sparkSession, final SnowFlakeTargetNode node, final String tableName) {
        final String jdbcURL = node.getSnowFlakeConnectionProp().entrySet().stream().map(k -> String.format("%s=%s", k.getKey(), k.getValue())).collect((Collector<? super Object, ?, String>)Collectors.joining("&"));
        final DataFrameReader read = sparkSession.read();
        node.getClass();
        read.format("net.snowflake.spark.snowflake").options((Map)node.getSnowFlakeConnectionProp()).table(tableName);
        return sparkSession.read().jdbc(jdbcURL, tableName, node.getSnowFlakeConnectionProp());
    }
    
    public void executeStmts(final LoadTarget node, final List<String> stmts) throws SnowFlakeExecutionException {
        for (final String stmt : stmts) {
            try {
                this.snowflakeSession.executeQuery(stmt, resultSet -> this.snowflakeSession.getConnection().close());
            }
            catch (SQLException e) {
                SnowFlakeTargetExecutor.LOGGER.error("Exception {}", (Object)AwbUtil.getErrorMessage((Throwable)e));
                final IWRunTimeException runTimeException = new IWRunTimeException("{QUERY EXECUTION ERROR}", (Throwable)e);
                Throwables.propagate((Throwable)runTimeException);
            }
        }
    }
    
    public void performTableMerge(final SnowFlakeTargetNode node) throws SnowFlakeExecutionException {
        final String mergeTablesStmt = SnowFlakeTargetTranslator.getMergeTableStmt(node.getDatabaseName(), node.getSchemaName(), node.getTableName(), node.getDatabaseName(), node.getSchemaName(), node.getStagingTableName(), node.getColumnMap(), node.getNaturalKeyColumns());
        SnowFlakeTargetExecutor.LOGGER.debug("Using Staging table {}", (Object)node.getStagingTableName());
        SnowFlakeTargetExecutor.LOGGER.info("Merge table sql: {}" + mergeTablesStmt);
        SnowFlakeTargetExecutor.LOGGER.info("Executing: {}" + mergeTablesStmt);
        try {
            this.snowflakeSession.executeQuery(mergeTablesStmt, resultSet -> {
                if (resultSet != null) {
                    while (resultSet.next()) {
                        SnowFlakeTargetExecutor.LOGGER.debug("Inserted records for table : {}", (Object)resultSet.getLong(1));
                        SnowFlakeTargetExecutor.LOGGER.debug("Updated records for table : {}", (Object)resultSet.getLong(2));
                    }
                }
            });
        }
        catch (SQLException e) {
            e.printStackTrace();
        }
        final List<String> postActions = new LinkedList<String>();
        postActions.add(SnowFlakeTargetTranslator.getDropTableStmt(node.getDatabaseName(), node.getSchemaName(), node.getStagingTableName()));
        try {
            this.executeStmts((LoadTarget)node, postActions);
        }
        catch (SnowFlakeExecutionException e2) {
            SnowFlakeTargetExecutor.LOGGER.error("Failed to perform merge operation on table {}.", (Object)node.getTableName());
            Throwables.propagate((Throwable)e2);
            try {
                this.snowflakeSession.getConnection().close();
            }
            catch (SQLException e3) {
                e3.printStackTrace();
            }
        }
        finally {
            try {
                this.snowflakeSession.getConnection().close();
            }
            catch (SQLException e4) {
                e4.printStackTrace();
            }
        }
    }
    
    public static Map<String, String> getSchema(final Dataset<Row> dataset) {
        final Map<String, String> columnMap = (Map<String, String>)Maps.newLinkedHashMap();
        final StructType datasetSchema = dataset.schema();
        final StructField[] fields2;
        final StructField[] fields = fields2 = datasetSchema.fields();
        for (final StructField col : fields2) {
            columnMap.put(col.name().toLowerCase(), col.dataType().simpleString().toLowerCase());
        }
        return columnMap;
    }
    
    public Map<String, String> getSfTableSchema(final SparkSession sparkSession, final SnowFlakeTargetNode node) {
        final String schemaStmt = String.format("SELECT * FROM  \"%s\".\"%s\".%s WHERE 1=0", node.getDatabaseName(), node.getSchemaName(), node.getTableName());
        final Dataset schemaRdd = this.executeStmt(sparkSession, node, schemaStmt);
        return getSchema((Dataset<Row>)schemaRdd);
    }
    
    private String getSfCompatibleDataType(final String dataType) {
        final SnowFlakeTargetTranslator translator = this.translator;
        final String newDataType = SnowFlakeTargetTranslator.getSfCompatibleTypesMap().get(dataType.trim().toLowerCase());
        return (newDataType != null) ? newDataType : dataType;
    }
    
    public boolean isSnowflakeTargetCompatible(final SparkSession session, final SnowFlakeTargetNode node) {
        final Map<String, String> targetColumnMap = this.getSfTableSchema(session, node);
        SnowFlakeTargetExecutor.LOGGER.debug("Target table column list: {}", (Object)targetColumnMap);
        SnowFlakeTargetExecutor.LOGGER.debug("This table column list: {}", (Object)node.getColumnMap());
        if (targetColumnMap.size() != node.getColumnMap().size()) {
            SnowFlakeTargetExecutor.LOGGER.error("Target table {} and this table column {} ;  count mismatch", (Object)targetColumnMap.size(), (Object)node.getColumnMap().size());
            return false;
        }
        for (final Map.Entry<String, String> entry : targetColumnMap.entrySet()) {
            final String targetCol = entry.getValue();
            final String nodeColumn = node.getColumnMap().get(entry.getKey());
            if (nodeColumn == null) {
                SnowFlakeTargetExecutor.LOGGER.error("SnowFlake Target table column {} does not exist in this target", (Object)entry.getKey());
                return false;
            }
            final String thisCol = this.getSfCompatibleDataType(node.getColumnMap().get(entry.getKey()));
            if (!targetCol.equals(thisCol)) {
                return false;
            }
        }
        return true;
    }
    
    public long getSfTableRowCount(final SnowFlakeTargetNode node) throws SnowFlakeExecutionException {
        final String sql = this.translator.getSFtableRowCountSql(node);
        try {
            this.snowflakeSession.executeQuery(sql, resultSet -> {
                resultSet.next();
                this.rowCount = resultSet.getLong(1);
            });
            return this.rowCount;
        }
        catch (SQLException e) {
            e.printStackTrace();
            Throwables.propagate((Throwable)e);
            try {
                this.snowflakeSession.getConnection().close();
            }
            catch (SQLException e) {
                e.printStackTrace();
            }
        }
        finally {
            try {
                this.snowflakeSession.getConnection().close();
            }
            catch (SQLException e2) {
                e2.printStackTrace();
            }
        }
        return this.rowCount;
    }
    
    public boolean isSfTableExists(final SnowFlakeTargetNode node) throws SQLException {
        final String sql = this.translator.getIfTableExistSql(node);
        final StringBuilder output = new StringBuilder("0");
        try {
            this.snowflakeSession.executeQuery(sql, resultSet -> {
                if (resultSet != null) {
                    while (resultSet.next()) {
                        final Long count = resultSet.getLong(1);
                        output.append(count.toString());
                        SnowFlakeTargetExecutor.LOGGER.debug("Number of table count : {}", (Object)count.toString());
                    }
                }
            });
            return Long.parseLong(output.toString().trim()) == 1L;
        }
        catch (SQLException e) {
            SnowFlakeTargetExecutor.LOGGER.error("Failed executing SnowFlake table row count sql");
            Throwables.propagate((Throwable)e);
            try {
                this.snowflakeSession.getConnection().close();
            }
            catch (SQLException e) {
                e.printStackTrace();
            }
        }
        finally {
            try {
                this.snowflakeSession.getConnection().close();
            }
            catch (SQLException e2) {
                e2.printStackTrace();
            }
        }
        return false;
    }
    
    protected void overWrite(final LoadTarget targetNode) {
        final List<String> preActionsSqlList = this.translator.getpreActionsSqlList(LoadTarget.SyncMode.OVERWRITE);
        final List<String> postActionsSqlList = new LinkedList<String>();
        try {
            this.executeStmts(targetNode, preActionsSqlList);
        }
        catch (SnowFlakeExecutionException e) {
            e.printStackTrace();
        }
        preActionsSqlList.clear();
        this.writeTableToSnowFlake(targetNode.getTableName(), SaveMode.Overwrite, preActionsSqlList, postActionsSqlList);
    }
    
    protected void merge(final LoadTarget targetNode) {
        try {
            if (!this.isSfTableExists(this.snowFlakeTargetNode)) {
                SnowFlakeTargetExecutor.LOGGER.info("SnowFlake target table {} does not exist. Switching to OVERWRITE mode.", (Object)this.snowFlakeTargetNode.getTableName());
                this.snowFlakeTargetNode.setSyncMode(LoadTarget.SyncMode.OVERWRITE);
                this.overWrite((LoadTarget)this.snowFlakeTargetNode);
                return;
            }
            if (!this.isSnowflakeTargetCompatible(this.spark, (SnowFlakeTargetNode)targetNode)) {
                throw new SnowFlakeExecutionException(String.format("SnowFlake target table %s is not compatible to perform %s. Please check pipeline.", this.snowFlakeTargetNode.getTableName(), this.snowFlakeTargetNode.getSyncMode()));
            }
        }
        catch (SQLException | SnowFlakeExecutionException ex2) {
            final Exception ex;
            final Exception e = ex;
            e.printStackTrace();
            Throwables.propagate((Throwable)e);
        }
        final List<String> preActionsSqlList = this.translator.getpreActionsSqlList(LoadTarget.SyncMode.OVERWRITE);
        final List<String> postActionsSqlList = new LinkedList<String>();
        try {
            this.executeStmts((LoadTarget)this.snowFlakeTargetNode, preActionsSqlList);
        }
        catch (SnowFlakeExecutionException e2) {
            e2.printStackTrace();
        }
        preActionsSqlList.clear();
        this.writeTableToSnowFlake(this.snowFlakeTargetNode.getStagingTableName(), SaveMode.Overwrite, preActionsSqlList, postActionsSqlList);
        try {
            this.performTableMerge(this.snowFlakeTargetNode);
        }
        catch (SnowFlakeExecutionException e2) {
            SnowFlakeTargetExecutor.LOGGER.error("Exception while merging the SnowFlake table : {}", (Object)e2.getMessage());
            Throwables.propagate((Throwable)e2);
        }
    }
    
    protected void append(final LoadTarget targetNode) {
        try {
            if (!this.isSfTableExists(this.snowFlakeTargetNode)) {
                SnowFlakeTargetExecutor.LOGGER.info("SnowFlake target table {} does not exist. Switching to OVERWRITE mode.", (Object)this.snowFlakeTargetNode.getTableName());
                this.snowFlakeTargetNode.setSyncMode(LoadTarget.SyncMode.OVERWRITE);
                this.overWrite((LoadTarget)this.snowFlakeTargetNode);
                return;
            }
            if (!this.isSnowflakeTargetCompatible(this.spark, (SnowFlakeTargetNode)targetNode)) {
                throw new SnowFlakeExecutionException(String.format("SnowFlake target table %s is not compatible to perform %s. Please check pipeline.", this.snowFlakeTargetNode.getTableName(), this.snowFlakeTargetNode.getSyncMode()));
            }
            final List<String> preActionsSqlList = this.translator.getpreActionsSqlList(LoadTarget.SyncMode.APPEND);
            final List<String> postActionsSqlList = new LinkedList<String>();
            this.executeStmts((LoadTarget)this.snowFlakeTargetNode, preActionsSqlList);
            preActionsSqlList.clear();
            this.writeTableToSnowFlake(this.snowFlakeTargetNode.getTableName(), SaveMode.Append, preActionsSqlList, postActionsSqlList);
        }
        catch (SQLException | SnowFlakeExecutionException ex2) {
            final Exception ex;
            final Exception e = ex;
            e.printStackTrace();
            Throwables.propagate((Throwable)e);
        }
    }
    
    protected void writeTableToSnowFlake(final String tableName, final SaveMode saveMode, final List<String> preActionsSqlList, final List<String> postActionsSqlList) {
        final DataFrameWriter write = this.translator.getDataset((PipelineNode)this.targetNode).write();
        this.snowFlakeTargetNode.getClass();
        write.format("net.snowflake.spark.snowflake").options((Map)this.snowFlakeTargetNode.getSnowFlakeConnectionProp()).option("sfDatabase", this.snowFlakeTargetNode.getDatabaseName()).option("sfSchema", this.snowFlakeTargetNode.getSchemaName()).mode(saveMode).option("dbtable", tableName).option("preactions", String.join(";", preActionsSqlList)).option("postactions", String.join(";", postActionsSqlList)).save();
        SnowFlakeTargetExecutor.LOGGER.debug("entering post validation");
        try {
            this.postValidation();
        }
        catch (SnowFlakeExecutionException e) {
            e.printStackTrace();
            Throwables.propagate((Throwable)e);
        }
        SnowFlakeTargetExecutor.LOGGER.info("Completed writing to target table {}.{}.{}", new Object[] { this.snowFlakeTargetNode.getDatabaseName(), this.snowFlakeTargetNode.getSchemaName(), tableName });
    }
    
    protected boolean shouldUpdateMetaStore() {
        return true;
    }
    
    protected List<ColumnProperties> getTargetSchema() {
        final List<ColumnProperties> columns = new ArrayList<ColumnProperties>();
        final StructType schema = SparkMetaDataUtils.getSchemaForTarget(this.sparkTranslatorState, (PipelineNode)this.snowFlakeTargetNode);
        int i = 1;
        SnowFlakeTargetExecutor.LOGGER.debug("Schema ,{}", (Object)schema.toString());
        final StringBuilder columnList = new StringBuilder();
        columnList.append("Columns : ");
        for (final StructField field : schema.fields()) {
            final ColumnProperties column = new ColumnProperties();
            final String origColumnName = AwbUtil.removeTableNameFromColumnName(this.targetNode.getTableName(), field.name());
            columnList.append(origColumnName + ",");
            String columnTypeWithoutPrecision;
            final String sqlType = columnTypeWithoutPrecision = field.dataType().sql().toLowerCase();
            final int precisionIndex = columnTypeWithoutPrecision.indexOf("(");
            if (precisionIndex != -1) {
                columnTypeWithoutPrecision = columnTypeWithoutPrecision.substring(0, precisionIndex);
            }
            column.setName(field.name());
            column.setType(sqlType);
            column.setPrecision(Integer.valueOf(SparkMetaDataUtils.getPrecision(sqlType)));
            column.setSqlType(Integer.valueOf(SparkMetaDataUtils.getSqlType(columnTypeWithoutPrecision)));
            column.setAt(Integer.valueOf(i++));
            column.setSize(Integer.valueOf(SparkMetaDataUtils.getSize(sqlType)));
            column.setRole("o");
            column.setAuditColumn(Boolean.valueOf(AwbUtil.isAuditColumn(origColumnName)));
            columns.add(column);
        }
        SnowFlakeTargetExecutor.LOGGER.trace("Columns {}", (Object)columnList.toString());
        SnowFlakeTargetExecutor.LOGGER.trace("Columns count ,{}", (Object)columns.size());
        return columns;
    }
    
    protected long getTargetRowCount() {
        return this.rowCount;
    }
    
    protected Map<String, Long> getModifiedRecord() {
        final Map<String, Long> modifiedRecords = (Map<String, Long>)Maps.newHashMap();
        modifiedRecords.put("RowsInserted", 0L);
        modifiedRecords.put("RowsUpdated", 0L);
        modifiedRecords.put("RowsDeleted", 0L);
        if (this.snowFlakeTargetNode.getSyncMode() == LoadTarget.SyncMode.OVERWRITE) {
            return modifiedRecords;
        }
        if (this.snowFlakeTargetNode.getSyncMode() == LoadTarget.SyncMode.MERGE) {
            modifiedRecords.put("RowsInserted", this.insertedRecords.get());
            modifiedRecords.put("RowsUpdated", this.updatedRecords.get());
        }
        else if (this.snowFlakeTargetNode.getSyncMode() == LoadTarget.SyncMode.APPEND) {
            final Dataset<Row> dataset = this.sparkTranslatorState.getDataFrame((PipelineNode)this.snowFlakeTargetNode).getDataset();
            final List<Row> result = (List<Row>)dataset.groupBy(new Column[0]).count().collectAsList();
            modifiedRecords.put("RowsInserted", result.get(0).getLong(0));
        }
        SnowFlakeTargetExecutor.LOGGER.debug(" Inserted records {}", (Object)modifiedRecords.get("RowsInserted"));
        SnowFlakeTargetExecutor.LOGGER.debug(" Updated records {}", (Object)modifiedRecords.get("RowsUpdated"));
        SnowFlakeTargetExecutor.LOGGER.debug(" Deleted records {}", (Object)modifiedRecords.get("RowsDeleted"));
        return modifiedRecords;
    }
    
    protected MetaStoreObjectGenerator getMetastoreObjectConverter(final LoadTarget node, final RequestContext requestContext, final List<ColumnProperties> tableColumns) {
        return (MetaStoreObjectGenerator)new SnowflakeMetastoreObjectConvertor(node, requestContext, (List)tableColumns);
    }
    
    private void postValidation() throws SnowFlakeExecutionException {
        this.rowCount = 0L;
        if (AwbConfigs.validateRowcount(this.reqCtx)) {
            SnowFlakeTargetExecutor.LOGGER.debug("Validate row count value is {}", (Object)AwbConfigs.validateRowcount(this.reqCtx));
            this.rowCount = this.getSfTableRowCount(this.snowFlakeTargetNode);
            SnowFlakeTargetExecutor.LOGGER.info("Target table row count: {}", (Object)this.rowCount);
        }
    }
    
    static {
        LOGGER = LoggerFactory.getLogger((Class)SnowFlakeTargetExecutor.class);
    }
}
