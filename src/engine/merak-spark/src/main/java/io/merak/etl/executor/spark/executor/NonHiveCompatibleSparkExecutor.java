package io.merak.etl.executor.spark.executor;

import io.merak.etl.dag.impl.*;
import com.google.common.base.*;
import io.merak.etl.task.impl.*;
import io.merak.etl.sdk.task.*;
import io.merak.etl.utils.config.*;
import io.merak.etl.executor.spark.executor.extensions.*;
import io.merak.etl.executor.impl.*;
import io.merak.etl.sdk.dag.api.*;
import java.io.*;
import io.merak.etl.sdk.engine.*;
import java.util.*;
import io.merak.adapters.filesystem.*;
import io.merak.etl.pipeline.dto.*;

import org.apache.spark.sql.*;
import com.google.common.collect.*;

public class NonHiveCompatibleSparkExecutor extends SparkBatchExecutor
{
    @Override
    protected void tableOverwrite(final String workingTableName, final String workingTableLocation) {
        this.LOGGER.debug(" overwrite mode creating hive non compatible spark target {} {} ", (Object)workingTableName, (Object)workingTableLocation);
        final DataFrameWriter dataFrameWriter = this.translator.overWriteNonHiveCompatible(workingTableName, workingTableLocation);
        this.executeSparkStatement(this.translator.generateDatabaseStmts(workingTableName));
        dataFrameWriter.saveAsTable(workingTableName);
    }
    
    @Override
    protected void preProcessIncremental(final String tableName, final String workingTableName, final String workingTableLocation) {
        this.hdfsUtils.deleteDirIfExists(workingTableLocation);
        final DataFrameWriter dataFrameWriter = this.translator.mergeNonHiveCompatible(workingTableName, workingTableLocation);
        this.executeSparkStatement(this.translator.generateDatabaseStmts(workingTableName));
        dataFrameWriter.saveAsTable(workingTableName);
    }
    
    @Override
    protected void merge(final Set<String> changedPartitions, final Set<String> newPartitions) {
        this.LOGGER.debug("Merging in Non hive compatible mode");
        if (!this.targetNode.isHiveCompatible() && this.targetNode.getPrimaryPartitionColumns().isEmpty()) {
            changedPartitions.clear();
            changedPartitions.add("");
            newPartitions.clear();
        }
    }
    
    @Override
    protected void mergePartition(final String partition, final int mergeFileNum) throws IOException {
        final Dag<TaskNode> taskDag = (Dag<TaskNode>)new DagImpl();
        this.LOGGER.trace("Checking Incremental Partition :{}", (Object)partition);
        final String incrementalDataStoragePath = String.format("%s/%s", AwbPathUtil.getIncrementalDeltaStoragePath(this.targetNode), partition);
        final Set<String> incrementalBuckets = SparkHelper.getBucketPatterns(incrementalDataStoragePath, this.iwFileSystem);
        if (incrementalBuckets.size() == 0) {
            this.LOGGER.info("Skipping merge as there are no incremental bucket changes.");
        }
        incrementalBuckets.forEach(i -> this.LOGGER.trace("Incremental buckets: {}", (Object)i));
        final Set<String> newBuckets = (Set<String>)Sets.newHashSet();
        this.LOGGER.info("current table location while doing merge is {}", (Object)this.targetNode.getCurrentTableLoc());
        final String currentPartitionPath = String.format("%s/%s", this.targetNode.getCurrentTableLoc(), partition);
        final Set<String> existingBuckets = SparkHelper.getBucketPatterns(currentPartitionPath, this.iwFileSystem);
        existingBuckets.forEach(i -> this.LOGGER.trace("Existing partition: {}", (Object)i));
        final Set<String> changedBuckets = (Set<String>)Sets.newHashSet();
        this.LOGGER.trace("Current table loc: {}", (Object)this.targetNode.getCurrentTableLoc());
        existingBuckets.forEach(i -> this.LOGGER.trace("Existing bucket: {}", (Object)i));
        for (final String j : incrementalBuckets) {
            if (existingBuckets.contains(j)) {
                changedBuckets.add(j);
            }
            else {
                newBuckets.add(j);
            }
        }
        for (final String bucket : incrementalBuckets) {
            final String bucket2;
            taskDag.addDisconnectedNode((DagNode)new RunnableTaskNode(() -> {
                try {
                    this.mergeBucket(bucket2, partition);
                }
                catch (Exception e) {
                    this.LOGGER.error("Error while merging partition '{}':\n{}", (Object)partition, (Object)Throwables.getStackTraceAsString((Throwable)e));
                    Throwables.propagate((Throwable)e);
                }
                return;
            }));
        }
        taskDag.makeAsEndNodes((Set)Sets.newHashSet((Object[])new TaskNode[] { (TaskNode)new RunnableTaskNode(() -> this.completeBucketMerge(changedBuckets, newBuckets, partition)) }));
        new DefaultTaskDagExecutorImpl(this.reqCtx, AwbConfigs.getMergeExecPoolSize(), 0).execute((Dag)taskDag);
    }
    
    @Override
    protected SupportedExecutionEngines computeSupportedEngine(final List<BatchEngine> readbatchEngineList) {
        this.LOGGER.trace("target is non hive compatible");
        return new SupportedExecutionEngines((List)readbatchEngineList, (List)Arrays.asList(BatchEngine.SPARK));
    }
    
    @Override
    protected void postMergeProcess(final Set<String> changedPartitions, final Set<String> newPartitions, final Map<String, String> columnTypes, final Location mainLocation, final Location mergedLocation, final Location deltaLocation) throws IOException {
        if (this.targetNode.isHiveCompatible() || !this.targetNode.getPrimaryPartitionColumns().isEmpty()) {
            this.moveMergedPartitions(mainLocation, deltaLocation, (Set)newPartitions);
        }
        this.hdfsUtils.deleteDirIfExists(AwbPathUtil.getIncrementalDeltaStoragePath(this.targetNode));
        this.postMergeNonHiveCompatible();
    }
    
    @Override
    public void executeAnalyzeStatements(final TargetNode node) {
        this.executeSparkStatement(String.format("REFRESH TABLE `%s`.`%s`", node.getSchemaName(), node.getTableName()));
    }
    
    private void completeBucketMerge(final Set<String> changedBuckets, final Set<String> newBuckets, final String partition) {
        final String mainTableLoc = String.format("%s/%s", this.targetNode.getCurrentTableLoc(), partition);
        try {
            SparkHelper.deleteUpdatebucket(changedBuckets, mainTableLoc, this.iwFileSystem);
        }
        catch (IOException e) {
            this.LOGGER.debug("Error while deleting old updated files from main table location for partition {} ", (Object)partition);
            e.printStackTrace();
        }
        final String deltaPath = String.format("%s/%s", AwbPathUtil.getIncrementalDeltaStoragePath(this.targetNode), partition);
        try {
            SparkHelper.moveDeltaBucket(deltaPath, mainTableLoc, newBuckets, this.iwFileSystem);
        }
        catch (IOException e2) {
            this.LOGGER.debug("Error while moving delta files to main table location for partition {} ", (Object)partition);
            e2.printStackTrace();
        }
        final String mergedPath = String.format("%s/%s", AwbPathUtil.getIncrementalMergeStoragePath(this.targetNode), partition);
        try {
            SparkHelper.moveMergedBucket(mergedPath, mainTableLoc, this.iwFileSystem);
        }
        catch (IOException e3) {
            this.LOGGER.debug("Error while moving merged buckets to main table location for partitions : {}", (Object)partition);
            e3.printStackTrace();
        }
    }
    
    private void mergeBucket(final String bucket, final String partition) {
        this.LOGGER.debug("Merging bucket {}/{}", (Object)partition, (Object)bucket);
        String mainTablePath = String.format("%s/%s/%s", this.targetNode.getCurrentTableLoc(), partition, bucket);
        mainTablePath = SparkHelper.cleanUrl(mainTablePath);
        String deltaTablePath = String.format("%s/%s/%s", AwbPathUtil.getIncrementalDeltaStoragePath(this.targetNode), partition, bucket);
        deltaTablePath = SparkHelper.cleanUrl(deltaTablePath);
        String mergeParentPath = String.format("%s/%s", AwbPathUtil.getIncrementalMergeStoragePath(this.targetNode), partition);
        mergeParentPath = SparkHelper.cleanUrl(mergeParentPath);
        final Dataset<Row> deltaTable = this.getDataSet(deltaTablePath);
        this.LOGGER.debug("Existing bucket : {}", (Object)bucket);
        final Dataset<Row> mainTable = this.getDataSet(mainTablePath);
        final Dataset<Row> mergedJoin = this.targetNode.isSCD2() ? this.getSCD2Merge(mainTable, deltaTable) : this.getFullOuterJoin(mainTable, deltaTable);
        this.LOGGER.debug("Number of row in main table {} is {}", (Object)mainTablePath, (Object)mainTable.groupBy(new Column[0]).count().collectAsList().get(0).getLong(0));
        this.LOGGER.debug("Number of row in delta table {} is {}", (Object)deltaTablePath, (Object)deltaTable.groupBy(new Column[0]).count().collectAsList().get(0).getLong(0));
        String tempMergePath = String.format("%s/%s%d", mergeParentPath, "ziw_", SparkHelper.getBucketNumber(bucket));
        tempMergePath = SparkHelper.cleanUrl(tempMergePath);
        final DataFrameWriter dataFrameWriter = SparkHelper.getMergeTableWriter(this.targetNode, mergedJoin, tempMergePath);
        final long uniqueID = ((TaskResult)this.taskCtx.get("temp_tables")).incrementTempTableCount();
        final String workingTableName = String.format("`%s`.`%s_%d_%s`", this.targetNode.getSchemaName(), this.targetNode.getTableName(), uniqueID, "merged");
        final String dropTable = String.format("DROP TABLE IF EXISTS %s", workingTableName);
        this.executeSparkStatement(dropTable);
        dataFrameWriter.mode(SaveMode.Overwrite).saveAsTable(workingTableName);
        this.LOGGER.debug("Successfully Merged Partition {}/{}", (Object)partition, (Object)bucket);
    }
    
    private void postMergeNonHiveCompatible() {
        final List<String> statements = (List<String>)Lists.newArrayList();
        final long numOfTempTables = ((TaskResult)this.taskCtx.get("temp_tables")).getTempTableCount();
        for (int i = 1; i <= numOfTempTables; ++i) {
            final String tableName = String.format("%s_%d_%s", this.targetNode.getTableName(), i, "merged");
            statements.add(String.format("DROP TABLE IF EXISTS `%s`.`%s`", this.targetNode.getSchemaName(), tableName));
        }
        statements.add(String.format("REFRESH TABLE `%s`.`%s`", this.targetNode.getSchemaName(), this.targetNode.getTableName()));
        this.executeSparkStatement(statements);
    }
}
