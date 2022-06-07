package io.merak.etl.executor.spark.executor;

import io.merak.etl.executor.impl.*;

import java.util.concurrent.atomic.*;

import io.merak.etl.sdk.task.*;
import io.merak.etl.context.*;
import com.google.common.base.*;
import java.sql.*;
import org.apache.spark.sql.types.*;

import com.google.common.collect.*;

import scala.collection.*;
import io.merak.etl.pipeline.dto.*;
import org.apache.spark.sql.*;
import io.merak.metadata.impl.dto.tables.columns.*;
import io.merak.etl.executor.spark.exceptions.*;
import io.merak.etl.executor.spark.metadata.*;
import io.merak.etl.executor.spark.session.*;
import io.merak.etl.executor.spark.task.*;
import io.merak.etl.executor.spark.translator.*;
import io.merak.etl.executor.spark.translator.dto.*;
import io.merak.etl.executor.spark.utils.*;

import java.util.*;

import io.merak.etl.pipeline.dto.convertors.*;
import io.merak.etl.utils.config.*;
import org.slf4j.*;

public class AzureDWTargetExecutor extends LoadTargetExecutor
{
    private static final Logger LOGGER;
    protected AzureDWTargetTranslator translator;
    protected SparkSession spark;
    protected SparkTranslatorState sparkTranslatorState;
    protected SparkAzureDWTaskNode taskNode;
    protected AtomicLong insertedRecords;
    protected AtomicLong updatedRecords;
    protected AtomicLong deletedRecords;
    protected AzureDWSession azureDWSession;
    protected SCDHelper scdHelper;
    protected long rowCount;
    private AzureDWNode azureDWNode;
    
    public AzureDWTargetExecutor() {
        this.spark = null;
    }
    
    protected void init(final Task task, final RequestContext requestContext) {
        this.reqCtx = requestContext;
        this.taskNode = (SparkAzureDWTaskNode)task;
        this.targetNode = this.taskNode.getSink();
        this.azureDWNode = (AzureDWNode)this.targetNode;
        this.translator = (AzureDWTargetTranslator)((SparkAzureDWTaskNode)task).getSinkTranslator();
        this.spark = this.translator.getSpark();
        AzureDWTargetExecutor.LOGGER.info("{} :{}", (Object)this.azureDWNode.getAccessConfString(), (Object)this.azureDWNode.getAccessKey());
        this.spark.conf().set(this.azureDWNode.getAccessConfString(), this.azureDWNode.getAccessKey());
        this.azureDWSession = new AzureDWSession(this.reqCtx, this.azureDWNode);
        (this.sparkTranslatorState = (SparkTranslatorState)this.taskNode.getTranslatorState()).runTranslations();
        this.insertedRecords = new AtomicLong(0L);
        this.updatedRecords = new AtomicLong(0L);
        this.deletedRecords = new AtomicLong(0L);
    }
    
    private Dataset<Row> executeStmt(final SparkSession sparkSession, final AzureDWNode node, final String sql) throws SQLException {
        Dataset<Row> new_dataset = null;
        try {
            AzureDWTargetExecutor.LOGGER.info("executing sql stmt : {}", (Object)sql);
            final DataFrameReader read = sparkSession.read();
            node.getClass();
            new_dataset = (Dataset<Row>)read.format("com.databricks.spark.sqldw").option("url", node.getURL()).option("tempDir", node.getTempDir()).option("forwardSparkAzureStorageCredentials", "true").option("query", sql).load();
        }
        catch (Exception e) {
            e.printStackTrace();
            Throwables.propagate((Throwable)e);
        }
        return new_dataset;
    }
    
    public static Map<String, String> getSchema(final Dataset<Row> dataset) {
        final Map<String, String> columnMap = (Map<String, String>)Maps.newLinkedHashMap();
        final StructType datasetSchema = dataset.schema();
        final StructField[] fields2;
        final StructField[] fields = fields2 = datasetSchema.fields();
        for (final StructField col : fields2) {
            columnMap.put(col.name().toLowerCase(), col.dataType().simpleString().toLowerCase());
        }
        AzureDWTargetExecutor.LOGGER.info("Target table column list: {}", (Object)columnMap);
        return columnMap;
    }
    
    public Map<String, String> getAzureDWTableSchema(final SparkSession sparkSession, final AzureDWNode node) {
        final String schemaStmt = String.format("SELECT * FROM  \"%s\".%s WHERE 1=0", node.getSchemaName(), node.getTableName());
        Dataset<Row> schemaRdd = null;
        try {
            schemaRdd = this.executeStmt(sparkSession, node, schemaStmt);
        }
        catch (SQLException e) {
            e.printStackTrace();
            Throwables.propagate((Throwable)e);
        }
        return getSchema(schemaRdd);
    }
    
    public boolean isAzureDWTargetCompatible(final SparkSession session, final AzureDWNode node) {
        final Map<String, String> targetColumnMap = this.getAzureDWTableSchema(session, node);
        AzureDWTargetExecutor.LOGGER.info("Target table column list: {}", (Object)targetColumnMap);
        AzureDWTargetExecutor.LOGGER.info("This table column list: {}", (Object)node.getColumnMap());
        if (targetColumnMap.size() != node.getColumnMap().size()) {
            AzureDWTargetExecutor.LOGGER.error("Target table {} and this table column {} ;  count mismatch", (Object)targetColumnMap.size(), (Object)node.getColumnMap().size());
            return false;
        }
        for (final Map.Entry<String, String> entry : targetColumnMap.entrySet()) {
            final String targetCol = entry.getValue();
            final String nodeColumn = node.getColumnMap().get(entry.getKey());
            if (nodeColumn == null) {
                AzureDWTargetExecutor.LOGGER.error("AzureDW Target table column {} does not exist in this target", (Object)entry.getKey());
                return false;
            }
            if (!targetCol.equals(nodeColumn)) {
                return false;
            }
        }
        return true;
    }
    
    public long getAzureDWTableRowCount(final AzureDWNode node) throws AzureDWExecutionException {
        final String sql = this.translator.getAzureDWtableRowCountSql(node);
        try {
            final Dataset<Row> dataset = this.executeStmt(this.spark, node, sql);
            final Row[] result = (Row[])dataset.take(1);
            return this.rowCount = result[0].getInt(0);
        }
        catch (SQLException e) {
            e.printStackTrace();
            Throwables.propagate((Throwable)e);
            return this.rowCount;
        }
    }
    
    public boolean isAzureDWTableExists(final AzureDWNode node) throws SQLException {
        final String sql = this.translator.getIfTableExistSql(node);
        final StringBuilder output = new StringBuilder("0");
        try {
            final Dataset<Row> dataset = this.executeStmt(this.spark, node, sql);
            final Row[] result = (Row[])dataset.take(1);
            final int count = result[0].getInt(0);
            AzureDWTargetExecutor.LOGGER.debug("Number of table count : {}", (Object)count);
            return count == 1;
        }
        catch (SQLException e) {
            e.printStackTrace();
            AzureDWTargetExecutor.LOGGER.error("Failed executing AzureDW table row count sql");
            Throwables.propagate((Throwable)e);
            return false;
        }
    }
    
    public List<String> getAzureDWSelectColumns() {
        final List<String> columns = (List<String>)Lists.newArrayList();
        final List<String> list;
        this.azureDWNode.getOutputEntities().forEach(c -> {
            if (c.shouldExclude()) {
                list.add(String.format("D.%s AS %s", c.getOutputName((PipelineNode)this.azureDWNode), c.getOutputName((PipelineNode)this.azureDWNode)));
            }
            else if (!c.getGivenName().equals("ziw_row_id")) {
                list.add(String.format("if(D.%s IS NULL, T.%s, D.%s) AS %s ", c.getOutputName((PipelineNode)this.azureDWNode), c.getOutputName((PipelineNode)this.azureDWNode), c.getOutputName((PipelineNode)this.azureDWNode), c.getOutputName((PipelineNode)this.azureDWNode)));
            }
            return;
        });
        return columns;
    }
    
    private Column getJoinExpression(final Dataset<Row> mainTable, final Dataset<Row> deltaTable) {
        Column column = null;
        for (final Entity entity : this.azureDWNode.getNaturalKeyColumns()) {
            final String inputColumn = entity.getGivenName();
            final String lookupColumn = entity.getGivenName();
            if (column == null) {
                column = mainTable.col(inputColumn).equalTo((Object)deltaTable.col(lookupColumn));
            }
            else {
                column.and(mainTable.col(inputColumn).equalTo((Object)deltaTable.col(lookupColumn)));
            }
        }
        return column;
    }
    
    protected Dataset<Row> getFullOuterJoin(final Dataset<Row> mainTable, final Dataset<Row> deltaTable) throws AzureDWExecutionException {
        final Column joinExp = this.getJoinExpression(mainTable, deltaTable);
        Dataset<Row> fullOuterJoin = (Dataset<Row>)mainTable.as("T").join(deltaTable.as("D"), joinExp, "fullouter");
        final List<String> columns = this.getAzureDWSelectColumns();
        columns.forEach(column -> AzureDWTargetExecutor.LOGGER.info("SCD1 Column {}", (Object)column));
        fullOuterJoin = (Dataset<Row>)fullOuterJoin.select((Seq)SparkUtils.getColumnSeqFromExpressions(this.spark, columns));
        return fullOuterJoin;
    }
    
    protected void overWrite(final LoadTarget targetNode) {
        AzureDWTargetExecutor.LOGGER.info("Doing overwrite");
        final List<String> preActionsSqlList = this.translator.getpreActionsSqlList(LoadTarget.SyncMode.OVERWRITE);
        final List<String> postActionsSqlList = new LinkedList<String>();
        final Dataset<Row> datasetOverwrite = this.translator.getDataset((PipelineNode)targetNode);
        this.writeTableToAzureDW(datasetOverwrite, this.azureDWNode.getSchemaName(), this.azureDWNode.getTableName(), SaveMode.Overwrite, preActionsSqlList, postActionsSqlList);
    }
    
    protected void merge(final LoadTarget targetNode) {
        try {
            if (!this.isAzureDWTableExists(this.azureDWNode)) {
                AzureDWTargetExecutor.LOGGER.info("Azure target table {} does not exist. Switching to OVERWRITE mode.", (Object)this.azureDWNode.getTableName());
                this.azureDWNode.setSyncMode(LoadTarget.SyncMode.OVERWRITE);
                this.overWrite((LoadTarget)this.azureDWNode);
                return;
            }
            if (!this.isAzureDWTargetCompatible(this.spark, (AzureDWNode)targetNode)) {
                throw new AzureDWExecutionException(String.format("Azure target table %s is not compatible to perform %s. Please check pipeline.", this.azureDWNode.getTableName(), this.azureDWNode.getSyncMode()));
            }
        }
        catch (SQLException | AzureDWExecutionException ex2) {
            final Exception ex;
            final Exception e = ex;
            e.printStackTrace();
            Throwables.propagate((Throwable)e);
        }
        final String sql = String.format("SELECT * FROM %s.%s", this.azureDWNode.getSchemaName(), this.azureDWNode.getTableName());
        Dataset<Row> mainTable = null;
        try {
            mainTable = this.executeStmt(this.spark, this.azureDWNode, sql);
            mainTable = (Dataset<Row>)mainTable.withColumn("ZIW_ROW_ID".toLowerCase(), SparkUtils.getColumnFromExp(this.spark, this.translator.getRowIdExpr()));
        }
        catch (SQLException e2) {
            e2.printStackTrace();
            AzureDWTargetExecutor.LOGGER.error("failed to read from main table ");
            Throwables.propagate((Throwable)e2);
        }
        Dataset<Row> deltaTable = this.translator.getDataset((PipelineNode)targetNode);
        deltaTable = (Dataset<Row>)deltaTable.withColumn("ZIW_ROW_ID".toLowerCase(), SparkUtils.getColumnFromExp(this.spark, this.translator.getRowIdExpr()));
        Dataset<Row> mergedJoin = null;
        try {
            mergedJoin = this.getFullOuterJoin(mainTable, deltaTable);
        }
        catch (AzureDWExecutionException e3) {
            AzureDWTargetExecutor.LOGGER.error("Exception while merging the AzureDW table : {}", (Object)e3.getMessage());
            Throwables.propagate((Throwable)e3);
        }
        final List<String> preActionsSqlList = this.translator.getpreActionsSqlList(LoadTarget.SyncMode.OVERWRITE);
        final List<String> postActionsSqlList = this.translator.getpostActionsSqlList(LoadTarget.SyncMode.MERGE);
        this.writeTableToAzureDW(mergedJoin, this.azureDWNode.getSchemaName(), this.azureDWNode.getStagingTableName(), SaveMode.Overwrite, preActionsSqlList, postActionsSqlList);
    }
    
    protected void append(final LoadTarget targetNode) {
        try {
            if (!this.isAzureDWTableExists(this.azureDWNode)) {
                AzureDWTargetExecutor.LOGGER.info("AzureSQL DataWarehouse target table {} does not exist. Switching to OVERWRITE mode.", (Object)this.azureDWNode.getTableName());
                this.azureDWNode.setSyncMode(LoadTarget.SyncMode.OVERWRITE);
                this.overWrite((LoadTarget)this.azureDWNode);
                return;
            }
            if (!this.isAzureDWTargetCompatible(this.spark, (AzureDWNode)targetNode)) {
                throw new AzureDWExecutionException(String.format("AzureSQL_DW target table %s is not compatible to perform %s. Please check pipeline.", this.azureDWNode.getTableName(), this.azureDWNode.getSyncMode()));
            }
            final List<String> preActionsSqlList = this.translator.getpreActionsSqlList(LoadTarget.SyncMode.APPEND);
            final List<String> postActionsSqlList = new LinkedList<String>();
            final Dataset<Row> datasetAppend = this.translator.getDataset((PipelineNode)targetNode);
            this.writeTableToAzureDW(datasetAppend, this.azureDWNode.getSchemaName(), this.azureDWNode.getTableName(), SaveMode.Append, preActionsSqlList, postActionsSqlList);
        }
        catch (SQLException | AzureDWExecutionException ex2) {
            final Exception ex;
            final Exception e = ex;
            e.printStackTrace();
            Throwables.propagate((Throwable)e);
        }
    }
    
    protected void writeTableToAzureDW(final Dataset<Row> dataframe, final String schemaName, final String tableName, final SaveMode saveMode, final List<String> preActionsSqlList, final List<String> postActionsSqlList) {
        if (this.azureDWNode.getIndexingType() == AzureDWNode.IndexingType.non_clustered && saveMode == SaveMode.Overwrite) {
            postActionsSqlList.add(0, String.format("CREATE INDEX %s_%s ON %s.%s(%s)", schemaName, tableName, schemaName, tableName, this.azureDWNode.getIndexingColumns()));
            AzureDWTargetExecutor.LOGGER.info("non_clustered index is  : CREATE INDEX {}_{} ON {}.{}({})", new Object[] { schemaName, tableName, schemaName, tableName, this.azureDWNode.getIndexingColumns() });
        }
        final DataFrameWriter write = dataframe.write();
        this.azureDWNode.getClass();
        write.format("com.databricks.spark.sqldw").option("url", this.azureDWNode.getURL()).option("tempDir", this.azureDWNode.getTempDir()).option("forwardSparkAzureStorageCredentials", "true").option("dbtable", String.format("%s.%s", schemaName, tableName)).option("tableOptions", this.azureDWNode.getTableOptions()).option("preActions", String.join(";", preActionsSqlList)).option("postActions", String.join(";", postActionsSqlList)).mode(saveMode).save();
        AzureDWTargetExecutor.LOGGER.debug("entering post validation");
        try {
            this.postValidation();
        }
        catch (AzureDWExecutionException e) {
            e.printStackTrace();
            Throwables.propagate((Throwable)e);
        }
        AzureDWTargetExecutor.LOGGER.info("Completed writing to target table {}.{}", (Object)schemaName, (Object)tableName);
    }
    
    protected boolean shouldUpdateMetaStore() {
        return true;
    }
    
    protected List<ColumnProperties> getTargetSchema() {
        final List<ColumnProperties> columns = new ArrayList<ColumnProperties>();
        final StructType schema = SparkMetaDataUtils.getSchemaForTarget(this.sparkTranslatorState, (PipelineNode)this.azureDWNode);
        int i = 1;
        AzureDWTargetExecutor.LOGGER.debug("Schema ,{}", (Object)schema.toString());
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
        AzureDWTargetExecutor.LOGGER.trace("Columns {}", (Object)columnList.toString());
        AzureDWTargetExecutor.LOGGER.trace("Columns count ,{}", (Object)columns.size());
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
        if (this.azureDWNode.getSyncMode() == LoadTarget.SyncMode.OVERWRITE) {
            return modifiedRecords;
        }
        if (this.azureDWNode.getSyncMode() == LoadTarget.SyncMode.MERGE) {
            modifiedRecords.put("RowsInserted", this.insertedRecords.get());
            modifiedRecords.put("RowsUpdated", this.updatedRecords.get());
        }
        else if (this.azureDWNode.getSyncMode() == LoadTarget.SyncMode.APPEND) {
            final Dataset<Row> dataset = this.sparkTranslatorState.getDataFrame((PipelineNode)this.azureDWNode).getDataset();
            final List<Row> result = (List<Row>)dataset.groupBy(new Column[0]).count().collectAsList();
            modifiedRecords.put("RowsInserted", result.get(0).getLong(0));
        }
        return modifiedRecords;
    }
    
    protected MetaStoreObjectGenerator getMetastoreObjectConverter(final LoadTarget node, final RequestContext requestContext, final List<ColumnProperties> tableColumns) {
        return (MetaStoreObjectGenerator)new AzureDWMetastoreObjectConverter(node, requestContext, (List)tableColumns);
    }
    
    private void postValidation() throws AzureDWExecutionException {
        this.rowCount = 0L;
        if (AwbConfigs.validateRowcount(this.reqCtx)) {
            AzureDWTargetExecutor.LOGGER.debug("Validate row count value is {}", (Object)AwbConfigs.validateRowcount(this.reqCtx));
            this.rowCount = this.getAzureDWTableRowCount(this.azureDWNode);
            AzureDWTargetExecutor.LOGGER.info("Target table row count: {}", (Object)this.rowCount);
        }
    }
    
    static {
        LOGGER = LoggerFactory.getLogger((Class)AzureDWTargetExecutor.class);
    }
}
