package io.merak.etl.executor.spark.executor;

import io.merak.etl.sdk.engine.*;
import java.util.*;
import io.merak.adapters.filesystem.*;
import org.apache.spark.sql.*;
import java.io.*;
import java.sql.*;
import io.merak.etl.pipeline.dto.*;
import io.merak.etl.utils.config.*;

public class HiveCompatibleSparkExecutor extends SparkBatchExecutor
{
    @Override
    protected void tableOverwrite(final String workingTableName, final String workingTableLocation) {
        try {
            this.executeSparkStatement(this.translator.generateDatabaseStatement(this.reqCtx, this.targetNode.getSchemaName()));
        }
        catch (Exception e) {
            this.LOGGER.warn("Failed to create database {}", (Object)e.getMessage());
        }
        this.LOGGER.debug(" overwrite mode creating hive compatible spark target {} {} ", (Object)workingTableName, (Object)workingTableLocation);
        final List<String> stmts = this.translator.overWriteHiveCompatible(workingTableName, workingTableLocation);
        this.LOGGER.info("pre over write stmt is {}", (Object)stmts.toString());
        this.executeSparkStatement(stmts);
        this.insertIntoTable(workingTableName);
    }
    
    @Override
    protected void preProcessIncremental(final String tableName, final String workingTableName, final String workingTableLocation) {
        try {
            this.executeSparkStatement(this.translator.generateDatabaseStatement(this.reqCtx, this.targetNode.getSchemaName()));
        }
        catch (Exception e) {
            this.LOGGER.warn("Failed to create database {}", (Object)e.getMessage());
        }
        final List<String> stmts = this.translator.mergeHiveCompatible(workingTableName, workingTableLocation);
        this.executeSparkStatement(stmts);
        this.insertIntoTable(workingTableName);
    }
    
    @Override
    protected void merge(final Set<String> changedPartitions, final Set<String> newPartitions) {
    }
    
    @Override
    protected void mergePartition(final String partition, final int mergeFileNum) {
        this.LOGGER.trace("starting merge for partition {} . moving files now main location {} delta {} merged {} ", new Object[] { partition, this.targetNode.getCurrentTableLoc(), AwbPathUtil.getIncrementalDeltaStoragePath(this.targetNode), AwbPathUtil.getIncrementalMergeStoragePath(this.targetNode) });
        final String mainTablePath = String.format("%s/%s", this.targetNode.getCurrentTableLoc(), partition);
        final String deltaTablePath = String.format("%s/%s", AwbPathUtil.getIncrementalDeltaStoragePath(this.targetNode), partition);
        final String mergedPath = String.format("%s/%s", AwbPathUtil.getIncrementalMergeStoragePath(this.targetNode), partition);
        final Dataset<Row> mainTable = this.getDataSet(mainTablePath);
        final Dataset<Row> deltaTable = this.getDataSet(deltaTablePath);
        this.LOGGER.info("main table schema in mergePartition is {}", (Object)mainTable.schema().toString());
        this.LOGGER.info("delta table schema in mergePartition is {}", (Object)deltaTable.schema().toString());
        final Dataset<Row> mergedJoin = this.targetNode.isSCD2() ? this.getSCD2Merge(mainTable, deltaTable) : this.getFullOuterJoin(mainTable, deltaTable);
        this.LOGGER.trace(" saving file for partition {} number of merge files {} ", (Object)partition, (Object)mergeFileNum);
        mergedJoin.coalesce(mergeFileNum).write().mode(SaveMode.Overwrite).format(this.translator.getStorageFormat()).save(mergedPath);
    }
    
    @Override
    protected SupportedExecutionEngines computeSupportedEngine(final List<BatchEngine> readbatchEngineList) {
        this.LOGGER.trace("target is hive compatible");
        readbatchEngineList.addAll(Arrays.asList(BatchEngine.HIVE));
        return new SupportedExecutionEngines((List)readbatchEngineList, (List)Arrays.asList(BatchEngine.SPARK));
    }
    
    @Override
    protected void postMergeProcess(final Set<String> changedPartitions, final Set<String> newPartitions, final Map<String, String> columnTypes, final Location mainLocation, final Location mergedLocation, final Location deltaLocation) throws IOException, SQLException {
        final String path;
        final Dataset<Row> deltaTable;
        final long insertRecordCount;
        newPartitions.forEach(i -> {
            path = String.format("%s/%s", AwbPathUtil.getIncrementalDeltaStoragePath(this.targetNode), i);
            deltaTable = this.getDataSet(path);
            insertRecordCount = deltaTable.groupBy(new Column[0]).count().collectAsList().get(0).getLong(0);
            this.insertedRecords.addAndGet(insertRecordCount);
            return;
        });
        this.moveMergedPartitions(mainLocation, mergedLocation, (Set)changedPartitions);
        this.moveMergedPartitions(mainLocation, deltaLocation, (Set)newPartitions);
        this.LOGGER.debug("running post commands");
        this.postMergeHiveCompatible(changedPartitions, newPartitions, columnTypes);
    }
    
    @Override
    public void executeAnalyzeStatements(final TargetNode node) {
        this.executeSparkStatement(String.format("ANALYZE TABLE %s COMPUTE STATISTICS NOSCAN", this.getActualTable(this.targetNode)));
    }
    
    private void postMergeHiveCompatible(final Set<String> changedPartitions, final Set<String> newPartitions, final Map<String, String> columnTypes) throws SQLException {
        String stmt = String.format("MSCK REPAIR TABLE `%s`.`%s`", this.targetNode.getSchemaName(), this.targetNode.getTableName());
        try {
            this.LOGGER.debug("executing stmt {}", (Object)stmt);
            this.executeSparkStatement(stmt);
        }
        catch (Exception e) {
            this.LOGGER.warn("Exception {}", (Object)AwbUtil.getErrorMessage((Throwable)e));
        }
        stmt = String.format("REFRESH TABLE `%s`.`%s`", this.targetNode.getSchemaName(), this.targetNode.getTableName());
        try {
            this.LOGGER.debug("executing stmt {}", (Object)stmt);
            this.executeSparkStatement(stmt);
        }
        catch (Exception e) {
            this.LOGGER.warn("Exception {}", (Object)AwbUtil.getErrorMessage((Throwable)e));
        }
        final Map<String, Location> newPartitionSpec = this.translator.getPrimaryPartitionSpec(newPartitions, columnTypes);
        if (AwbConfigs.isComputeStatsEnabled()) {
            this.LOGGER.debug("computing stats ");
            final Map<String, Location> incrPartitionSpec = this.translator.getPrimaryPartitionSpec(changedPartitions, columnTypes);
            incrPartitionSpec.putAll(newPartitionSpec);
            this.updateTableStats(incrPartitionSpec);
        }
    }
    
    private void updateTableStats(final Map<String, Location> incrPartitionSpec) throws SQLException {
        final List<String> analyzeStmts = this.translator.getUpdateStatsStmt(incrPartitionSpec);
        this.executeSparkStatement(analyzeStmts);
    }
}
