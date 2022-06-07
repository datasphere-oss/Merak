package io.merak.etl.executor.spark.executor;

import java.util.concurrent.atomic.*;

import io.merak.etl.sdk.task.*;
import io.merak.etl.sdk.executor.*;
import com.google.common.base.*;
import java.io.*;
import io.merak.etl.sdk.engine.*;
import io.merak.adapters.filesystem.*;
import java.sql.*;
import io.merak.etl.pipeline.dto.*;
import scala.collection.*;
import org.apache.spark.sql.*;
import io.merak.etl.dag.impl.*;
import merak.tools.hadoop.mapreduce.*;
import merak.tools.progress.*;
import io.merak.etl.task.impl.*;
import io.merak.etl.executor.impl.*;
import io.merak.etl.sdk.dag.api.*;
import com.google.common.collect.*;
import io.merak.etl.utils.date.*;
import io.merak.etl.translator.*;
import io.merak.metadata.impl.dto.tables.columns.*;
import io.merak.etl.executor.spark.executor.extensions.*;
import io.merak.etl.executor.spark.metadata.*;
import io.merak.etl.executor.spark.session.*;
import io.merak.etl.executor.spark.task.*;
import io.merak.etl.executor.spark.translator.*;
import io.merak.etl.executor.spark.translator.dto.*;
import io.merak.etl.executor.spark.utils.*;

import java.util.*;
import io.merak.etl.utils.config.*;

import org.apache.spark.sql.types.*;

public abstract class SparkBatchExecutor extends BatchExecutor
{
    protected static final String TMP_TABLE = "temp_tables";
    protected static final String TMP_TAB_SUFFIX = "_iwtmp";
    protected BatchTargetTranslator translator;
    protected SparkSession spark;
    protected SparkTranslatorState sparkTranslatorState;
    protected SparkBatchTaskNode taskNode;
    protected AtomicLong insertedRecords;
    protected AtomicLong updatedRecords;
    protected AtomicLong deletedRecords;
    protected SCDHelper scdHelper;
    
    public SparkBatchExecutor() {
        this.spark = null;
    }
    
    protected void init() {
        this.taskNode = (SparkBatchTaskNode)this.task;
        this.translator = this.taskNode.getSinkTranslator();
        this.spark = this.translator.getSpark();
        this.executeSessionConfigs();
        (this.sparkTranslatorState = this.taskNode.getTranslatorState()).runTranslations();
        this.insertedRecords = new AtomicLong(0L);
        this.updatedRecords = new AtomicLong(0L);
        this.deletedRecords = new AtomicLong(0L);
        this.scdHelper = new SCDHelper(((SparkBatchTaskNode)this.task).getSink(), this.translator);
        this.taskCtx.set("temp_tables", (TaskOutput)new TaskResult());
    }
    
    protected void init(final TaskNode task, final TaskExecutorContext taskExecutorContext) {
        this.taskNode = (SparkBatchTaskNode)task;
        this.taskCtx = taskExecutorContext;
        this.translator = this.taskNode.getSinkTranslator();
        this.spark = this.translator.getSpark();
        this.executeSessionConfigs();
        (this.sparkTranslatorState = this.taskNode.getTranslatorState()).runTranslations();
        this.insertedRecords = new AtomicLong(0L);
        this.updatedRecords = new AtomicLong(0L);
        this.deletedRecords = new AtomicLong(0L);
        this.targetNode = ((SparkBatchTaskNode)task).getSink();
        this.scdHelper = new SCDHelper(((SparkBatchTaskNode)task).getSink(), this.translator);
        this.taskCtx.set("temp_tables", (TaskOutput)new TaskResult());
    }
    
    private void executeSessionConfigs() {
        final List<String> configsForPipeline = (List<String>)AwbConfigs.getBatchSparkConfigOverwrite(this.translator.getTranslatorContext().getRequestContext());
        configsForPipeline.add("hive.exec.dynamic.partition=true");
        configsForPipeline.add("hive.exec.dynamic.partition.mode=nonstrict");
        configsForPipeline.add("hive.optimize.sort.dynamic.partition=true");
        String stmt;
        configsForPipeline.forEach(config -> {
            try {
                if (config != null && !config.isEmpty()) {
                    stmt = String.format("SET %s", config);
                    this.LOGGER.debug("Executing Config {}", (Object)stmt);
                    this.executeSparkStatement(stmt, t -> {});
                }
            }
            catch (Exception e) {
                Throwables.propagate((Throwable)e);
            }
        });
    }
    
    protected abstract void tableOverwrite(final String p0, final String p1);
    
    protected abstract void merge(final Set<String> p0, final Set<String> p1);
    
    protected abstract void mergePartition(final String p0, final int p1) throws IOException;
    
    protected abstract void preProcessIncremental(final String p0, final String p1, final String p2);
    
    protected abstract SupportedExecutionEngines computeSupportedEngine(final List<BatchEngine> p0);
    
    protected abstract void postMergeProcess(final Set<String> p0, final Set<String> p1, final Map<String, String> p2, final Location p3, final Location p4, final Location p5) throws IOException, SQLException;
    
    protected void executePreOverWrite(final TargetNode targetNode) throws IOException {
        if (targetNode.isIntermediate()) {
            final String key = String.format("%s.%s", targetNode.getSchemaName(), targetNode.getTableName());
            this.LOGGER.debug("Caching dataset for {}", (Object)key);
            final Dataset<Row> tempDataSet = (Dataset<Row>)this.translator.getDf().cache();
            DataFrameCache.addToCache(key, tempDataSet);
            return;
        }
        final String workingTableLocation = this.dataAvailabilityEnforced() ? targetNode.getFutureTableLoc() : targetNode.getCurrentTableLoc();
        final String workingTableName = this.dataAvailabilityEnforced() ? this.getWorkingTable(targetNode) : this.getActualTable(targetNode);
        this.tableOverwrite(workingTableName, workingTableLocation);
    }
    
    public abstract void executeAnalyzeStatements(final TargetNode p0);
    
    protected Map<String, Long> getModifiedRecord() {
        final Map<String, Long> modifiedRecords = (Map<String, Long>)Maps.newHashMap();
        modifiedRecords.put("RowsInserted", 0L);
        modifiedRecords.put("RowsUpdated", 0L);
        modifiedRecords.put("RowsDeleted", 0L);
        if (this.targetNode.isOverwriteMode()) {
            return modifiedRecords;
        }
        if (this.targetNode.getSyncMode() == TargetNode.SyncMode.MERGE) {
            modifiedRecords.put("RowsInserted", this.insertedRecords.get());
            modifiedRecords.put("RowsUpdated", this.updatedRecords.get());
        }
        else if (this.targetNode.getSyncMode() == TargetNode.SyncMode.APPEND) {
            final Dataset<Row> dataset = this.translator.getDf();
            final List<Row> result = (List<Row>)dataset.groupBy(new Column[0]).count().collectAsList();
            modifiedRecords.put("RowsInserted", result.get(0).getLong(0));
        }
        this.LOGGER.debug(" Inserted records {}", (Object)modifiedRecords.get("RowsInserted"));
        this.LOGGER.debug(" Updated records {}", (Object)modifiedRecords.get("RowsUpdated"));
        this.LOGGER.debug(" Deleted records {}", (Object)modifiedRecords.get("RowsDeleted"));
        return modifiedRecords;
    }
    
    public void executeSparkStatement(final List<String> stmts) {
        this.LOGGER.debug("Executing spark sql statements.. ");
        for (final String sqlStatement : stmts) {
            this.LOGGER.debug("Executing statement : " + sqlStatement);
            try {
                this.executeSparkStatement(sqlStatement, t -> {});
            }
            catch (Exception e) {
                this.LOGGER.debug("Error while executing {}", (Object)sqlStatement);
                Throwables.propagate((Throwable)e);
            }
        }
    }
    
    protected void executeSparkStatement(final String sqlStatement, final DatasetProcessor processor) {
        try {
            final Dataset<Row> result = (Dataset<Row>)this.spark.sql(sqlStatement);
            processor.process(result);
        }
        catch (Exception e) {
            this.LOGGER.debug("Error while executing {}", (Object)sqlStatement);
            Throwables.propagate((Throwable)e);
        }
    }
    
    protected void executeSparkStatement(final String sqlStatement) {
        try {
            this.executeSparkStatement(sqlStatement, t -> {});
        }
        catch (Exception e) {
            this.LOGGER.debug("Error while executing {}", (Object)sqlStatement);
            Throwables.propagate((Throwable)e);
        }
    }
    
    protected void executePostOverWrite(final TargetNode targetNode) throws IOException {
        this.LOGGER.debug(String.format("Switching working table %s with actual table %s: ", this.getWorkingTable(targetNode), this.getActualTable(targetNode)));
        this.translator.switchTable(this.getWorkingTable(targetNode), this.getActualTable(targetNode));
        final Location existingTablePath = this.iwFileSystem.createLocation(targetNode.getCurrentTableLoc());
        this.LOGGER.debug("Executing delete of existing path {}", (Object)existingTablePath);
        this.hdfsUtils.deleteDirIfExists(existingTablePath.getFullPath());
        this.executeAnalyzeStatements(targetNode);
    }
    
    protected void insertIntoTable(final String workingTableName) {
        final Dataset<Row> targetSet = this.translator.getDf();
        this.LOGGER.info("dataframe schema is {}", (Object)targetSet.schema().toString());
        final int numberOfCalescePartition = AwbConfigs.getCoalescePartitions(this.translator.getTranslatorContext().getRequestContext());
        final Dataset<Row> targetSetOrdered = (Dataset<Row>)targetSet.select((Seq)SparkUtils.getColumnSeqFromExpressions(this.spark, this.scdHelper.getOrderedEntityGivenNames()));
        this.LOGGER.debug("Schema of targetSetOrdered {}", (Object)targetSetOrdered.schema());
        DataFrameWriter dfWriter;
        if (numberOfCalescePartition != -1) {
            this.LOGGER.debug("Found spark coalesce partitions as {} ", (Object)numberOfCalescePartition);
            dfWriter = targetSetOrdered.coalesce(numberOfCalescePartition).write().format(this.translator.getStorageFormat());
        }
        else {
            dfWriter = targetSetOrdered.write().format(this.translator.getStorageFormat());
        }
        if (this.targetNode.isAppendMode()) {
            this.LOGGER.debug("Appending records to the target table {} ", (Object)workingTableName);
            dfWriter.mode(SaveMode.Append).insertInto(workingTableName);
            this.LOGGER.debug("Successfully inserted data into {}", (Object)workingTableName);
        }
        else {
            this.LOGGER.debug("Inserting records to the target table {} ", (Object)workingTableName);
            dfWriter.mode(SaveMode.Overwrite).insertInto(workingTableName);
            this.LOGGER.debug("Successfully inserted data into {}", (Object)workingTableName);
        }
    }
    
    protected boolean isEligibleForMergeOptimization() {
        return true;
    }
    
    protected void executeIncremental(final TargetNode targetNode) throws IOException {
        final String tableName = "iwdf_delta_" + targetNode.getTableName();
        final String workingTableName = String.format("`%s`.`%s`", targetNode.getSchemaName(), tableName);
        final String workingTableLocation = AwbPathUtil.getIncrementalDeltaStoragePath(targetNode);
        this.preProcessIncremental(tableName, workingTableName, workingTableLocation);
    }
    
    protected void executeMerge(final Set<String> changedPartitions, final Set<String> newPartitions) {
        this.merge(changedPartitions, newPartitions);
        final Dag<TaskNode> taskDag = (Dag<TaskNode>)new DagImpl();
        final int numFilesMergeTarget = AwbConfigs.getSparkMergeFileNum();
        ProgressUtil.addToJobSummary(this.reqCtx.getJobId(), IWJob.JOB_TYPE.AWB_BATCH.name(), String.format("Merging rows to the main table for target: %s.", this.targetNode.getTableName()));
        for (final String partition : changedPartitions) {
            final String s;
            final int n;
            taskDag.addDisconnectedNode((DagNode)new RunnableTaskNode(() -> {
                try {
                    this.LOGGER.debug("trying to merge partiton {}", (Object)s);
                    this.mergePartition(s, n);
                    this.LOGGER.debug("done merge partiton {}", (Object)s);
                }
                catch (Exception e) {
                    this.LOGGER.error("Error while merging partition '{}':\n{}", (Object)s, (Object)Throwables.getStackTraceAsString((Throwable)e));
                    Throwables.propagate((Throwable)e);
                }
                return;
            }));
        }
        final Map<String, String> schema = this.translator.getColumnsFromSchema();
        taskDag.makeAsEndNodes((Set)Sets.newHashSet((Object[])new TaskNode[] { (TaskNode)new RunnableTaskNode(() -> this.completeMerge(changedPartitions, newPartitions, schema)) }));
        new DefaultTaskDagExecutorImpl(this.reqCtx, AwbConfigs.getMergeExecPoolSize(), 0).execute((Dag)taskDag);
    }
    
    protected SupportedExecutionEngines getSupportedEngines() {
        final List<BatchEngine> readbatchEngineList = (List<BatchEngine>)Lists.newArrayList();
        readbatchEngineList.add(BatchEngine.SPARK);
        return this.computeSupportedEngine(readbatchEngineList);
    }
    
    protected Dataset<Row> getSCD2Merge(final Dataset<Row> mainTable, final Dataset<Row> deltaTable) {
        final Column joinCond = SparkUtils.getColumnFromExp(this.spark, "T.ziw_row_id = D.ziw_row_id");
        final Dataset<Row> fullOuterJoin = (Dataset<Row>)mainTable.filter(SparkUtils.getColumnFromExp(this.spark, "ziw_active = true")).as("T").join(deltaTable.as("D"), joinCond, "fullouter");
        final String mainTableAlias = "T";
        final TargetNode.SCD2GRANULARITY scd2Granularity = this.targetNode.getSCD2Granularity();
        final Dataset<Row> dfDelta = (Dataset<Row>)fullOuterJoin.filter(SparkUtils.getColumnFromExp(this.spark, "D.ziw_row_id IS NOT NULL")).select((Seq)SparkUtils.getColumnSeqFromExpressions(this.spark, this.scdHelper.getSCD2DeltaTableColumns(scd2Granularity)));
        this.LOGGER.debug("Found SCD2 Granularity as {}", (Object)scd2Granularity);
        final String filterCondition = String.format("T.ziw_row_id is not null and D.ziw_row_id is not null %s%s", this.scdHelper.getTimeSimilarityUDFExp(scd2Granularity), this.scdHelper.isSCD2ColumnUpdated(true));
        this.LOGGER.debug("Filter Records based on condition {}", (Object)filterCondition);
        final Dataset<Row> dfUpdated = (Dataset<Row>)fullOuterJoin.filter(SparkUtils.getColumnFromExp(this.spark, filterCondition)).select((Seq)SparkUtils.getColumnSeqFromExpressions(this.spark, this.scdHelper.getSCD2OldRecordColumns(scd2Granularity)));
        this.updateRecordCount(dfUpdated, dfDelta);
        final Dataset<Row> dfOriginal = (Dataset<Row>)fullOuterJoin.filter(SparkUtils.getColumnFromExp(this.spark, "T.ziw_row_id is not null and D.ziw_row_id is null")).select((Seq)SparkUtils.getColumnSeqFromExpressions(this.spark, this.scdHelper.getSCD2DefaultColumns(mainTableAlias)));
        final Dataset<Row> dfInActive = (Dataset<Row>)mainTable.as(mainTableAlias).filter(SparkUtils.getColumnFromExp(this.spark, "ZIW_ACTIVE".toLowerCase() + " = false")).select((Seq)SparkUtils.getColumnSeqFromExpressions(this.spark, this.scdHelper.getSCD2DefaultColumns(mainTableAlias)));
        final Dataset<Row> unionSet = (Dataset<Row>)dfDelta.unionAll((Dataset)dfUpdated).unionAll((Dataset)dfOriginal).unionAll((Dataset)dfInActive);
        return unionSet;
    }
    
    private void updateRecordCount(final Dataset<Row> dfUpdated, final Dataset<Row> dfDelta) {
        final List<Row> updatedCount = (List<Row>)dfUpdated.groupBy(new Column[0]).count().collectAsList();
        final long updatedRecordCount = updatedCount.get(0).getLong(0);
        this.updatedRecords.addAndGet(updatedRecordCount);
        final long deltaCount = dfDelta.groupBy(new Column[0]).count().collectAsList().get(0).getLong(0);
        final long insertedRecordCount = deltaCount - updatedRecordCount;
        this.insertedRecords.addAndGet(insertedRecordCount);
        this.LOGGER.debug("Updated records for  {}", (Object)updatedRecordCount);
        this.LOGGER.debug("Delta records for {}", (Object)deltaCount);
        this.LOGGER.debug("Inserted record for {} ", (Object)insertedRecordCount);
    }
    
    public Dataset<Row> getDataSet(final String tablePath) {
        return (Dataset<Row>)this.spark.read().option("header", true).option("inferSchema", true).format(this.targetNode.getStorageFormat().toLowerCase()).load(tablePath);
    }
    
    protected Dataset<Row> getFullOuterJoin(final Dataset<Row> mainTable, final Dataset<Row> deltaTable) {
        final Column joinCond = SparkUtils.getColumnFromExp(this.spark, "T.ziw_row_id = D.ziw_row_id");
        Dataset<Row> fullOuterJoin = (Dataset<Row>)mainTable.as("T").join(deltaTable.as("D"), joinCond, "fullouter");
        final List<String> columns = this.scdHelper.getSCD1SelectColumns();
        columns.forEach(column -> this.LOGGER.info("SCD1 Column {}", (Object)column));
        fullOuterJoin = (Dataset<Row>)fullOuterJoin.select((Seq)SparkUtils.getColumnSeqFromExpressions(this.spark, columns));
        final Date jobStartTime = this.translator.getTranslatorContext().getRequestContext().getProcessingContext().getJobStartTime();
        final List<Row> result = (List<Row>)fullOuterJoin.filter(String.format("`%s` >= %s", "ZIW_UPDATED_TIMESTAMP".toLowerCase(), DateUtil.getExpressionForTimestamp(jobStartTime))).groupBy("ZIW_STATUS_FLAG".toLowerCase(), new String[0]).agg(SparkUtils.getColumnFromExp(this.spark, "COUNT(*) AS `record_count`"), (Seq)SparkUtils.getColumnSeqFromExpressions(this.spark, Lists.newArrayList())).collectAsList();
        for (final Row row : result) {
            final String operation = row.getString(0);
            final Long rowcount = row.getLong(1);
            final String upperCase = operation.toUpperCase();
            switch (upperCase) {
                case "I": {
                    this.insertedRecords.getAndAdd(rowcount);
                    continue;
                }
                case "U": {
                    this.updatedRecords.getAndAdd(rowcount);
                    continue;
                }
                case "D": {
                    this.deletedRecords.getAndAdd(rowcount);
                    continue;
                }
            }
        }
        return fullOuterJoin;
    }
    
    private void completeMerge(final Set<String> changedPartitions, final Set<String> newPartitions, final Map<String, String> columnTypes) {
        this.LOGGER.debug("completed merge . moving files now main location {} delta {} merged {} ", new Object[] { this.targetNode.getCurrentTableLoc(), AwbPathUtil.getIncrementalDeltaStoragePath(this.targetNode), AwbPathUtil.getIncrementalMergeStoragePath(this.targetNode) });
        final Location mainLocation = this.iwFileSystem.createLocation(this.targetNode.getCurrentTableLoc());
        final Location deltaLocation = this.iwFileSystem.createLocation(AwbPathUtil.getIncrementalDeltaStoragePath(this.targetNode));
        final Location mergedLocation = this.iwFileSystem.createLocation(AwbPathUtil.getIncrementalMergeStoragePath(this.targetNode));
        try {
            this.postMergeProcess(changedPartitions, newPartitions, columnTypes, mainLocation, mergedLocation, deltaLocation);
            this.hdfsUtils.deleteDirIfExists(deltaLocation.toString());
            this.hdfsUtils.deleteDirIfExists(mergedLocation.toString());
        }
        catch (Exception e) {
            this.LOGGER.error("Error while completing merge : {}", (Object)Throwables.getStackTraceAsString((Throwable)e));
            Throwables.propagate((Throwable)e);
        }
    }
    
    protected void executeAppend(final TargetNode targetNode) {
        this.executeSparkStatement(TranslatorUtils.getCreateDatabaseStatement(this.reqCtx, targetNode.getSchemaName()));
        final String tableName = String.format("`%s`.`%s`", targetNode.getSchemaName(), targetNode.getTableName());
        this.insertIntoTable(tableName);
    }
    
    protected List<ColumnProperties> getTargetSchema() {
        final List<ColumnProperties> columns = new ArrayList<ColumnProperties>();
        final StructType schema = this.translator.getSchemaForTarget();
        int i = 1;
        this.LOGGER.debug("Schema ,{}", (Object)schema.toString());
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
        this.LOGGER.trace("Columns {}", (Object)columnList.toString());
        this.LOGGER.trace("Columns count ,{}", (Object)columns.size());
        return columns;
    }
    
    protected long getTargetRowCount() {
        final Long[] rowCount = { 0L };
        Row[] count;
        final Object o;
        this.executeSparkStatement(this.translator.getCountQuery(), resultSet -> {
            try {
                count = (Row[])resultSet.take(1);
                o[0] = Long.valueOf(count[0].getLong(0));
            }
            catch (Exception e) {
                this.LOGGER.error("Error in getting count", (Throwable)e);
            }
            return;
        });
        return rowCount[0];
    }
    
    private boolean dataAvailabilityEnforced() {
        return !this.targetNode.isIntermediate();
    }
    
    protected String getActualTable(final TargetNode targetNode) {
        return String.format("`%s`.`%s`", targetNode.getSchemaName(), targetNode.getTableName());
    }
    
    private String getWorkingTable(final TargetNode targetNode) {
        return String.format("`%s`.`%s_%s`", targetNode.getSchemaName(), targetNode.getTableName(), "_iwtmp");
    }
}
