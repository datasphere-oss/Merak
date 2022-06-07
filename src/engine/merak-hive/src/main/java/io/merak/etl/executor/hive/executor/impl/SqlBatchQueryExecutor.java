package io.merak.etl.executor.hive.executor.impl;

import java.util.regex.*;
import java.io.*;
import merak.tools.ExceptionHandling.*;
import merak.tools.hadoop.mapreduce.*;
import merak.tools.progress.*;

import com.google.common.collect.*;

import io.merak.adapters.filesystem.*;
import io.merak.etl.context.*;
import io.merak.etl.dag.impl.*;
import io.merak.etl.executor.hive.task.*;
import io.merak.etl.executor.hive.translator.*;
import io.merak.etl.executor.impl.*;
import io.merak.etl.pipeline.dto.*;
import io.merak.etl.sdk.dag.api.*;
import io.merak.etl.sdk.engine.*;
import io.merak.etl.sdk.exceptions.*;
import io.merak.etl.sdk.session.*;
import io.merak.etl.sdk.task.*;
import io.merak.etl.session.*;
import io.merak.etl.task.impl.*;
import io.merak.etl.translator.*;
import io.merak.etl.utils.config.*;
import io.merak.metadata.impl.dto.tables.columns.*;

import java.util.*;
import java.util.function.*;
import com.google.common.base.*;

import java.sql.*;

public class SqlBatchQueryExecutor extends BatchExecutor
{
    private static final Pattern BUCKET_NAME_PATTERN;
    JdbcExecutionSession jdbcExecutionSession;
    Map<String, String> schema;
    BatchTargetTranslator batchTargetTranslator;
    
    protected void init() {
        this.jdbcExecutionSession = (JdbcExecutionSession)ExecutionEngineSessionFactory.getExecutionSession(this.reqCtx);
        this.batchTargetTranslator = (BatchTargetTranslator)this.task.getSinkTranslator();
        this.schema = this.getSchema();
    }
    
    protected void init(final RequestContext requestContext, final BatchQueryTaskNode taskNode) {
        this.reqCtx = requestContext;
        this.jdbcExecutionSession = (JdbcExecutionSession)ExecutionEngineSessionFactory.getExecutionSession(requestContext);
        this.task = (SinkTaskNode)taskNode;
        this.batchTargetTranslator = (BatchTargetTranslator)this.task.getSinkTranslator();
        this.targetNode = taskNode.getSink();
    }
    
    protected void executePreOverWrite(final TargetNode targetNode) throws ExecutionSessionException, IOException {
        final Location workingTableLocation = targetNode.isIntermediate() ? this.iwFileSystem.createLocation(targetNode.getCurrentTableLoc()) : this.iwFileSystem.createLocation(targetNode.getFutureTableLoc());
        this.executeDataBaseQuery(workingTableLocation);
    }
    
    protected Map<String, Long> getModifiedRecord() {
        final Map<String, Long> modifiedRecords = (Map<String, Long>)Maps.newHashMap();
        final Date jobStartTime = this.reqCtx.getProcessingContext().getJobStartTime();
        final String targetSchemaName = this.targetNode.getSchemaName();
        modifiedRecords.put("RowsInserted", 0L);
        modifiedRecords.put("RowsUpdated", 0L);
        modifiedRecords.put("RowsDeleted", 0L);
        if (this.targetNode.isOverwriteMode()) {
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
    
    protected void executePostOverWrite(final TargetNode targetNode) throws ExecutionSessionException, IOException {
        final List<String> renameStmts = this.batchTargetTranslator.switchTableStmts(false);
        if (AwbConfigs.isComputeStatsEnabled()) {
            renameStmts.add(this.batchTargetTranslator.getCreateStatsStmt());
        }
        this.LOGGER.debug("Executing switch-table statements: {}", (Object)String.join("; ", renameStmts));
        this.jdbcExecutionSession.executeStatements((List)renameStmts);
        final Location existingTablePath = this.iwFileSystem.createLocation(targetNode.getCurrentTableLoc());
        this.LOGGER.debug("Executing delete of existing path {}", (Object)existingTablePath);
        this.hdfsUtils.deleteDirIfExists(existingTablePath.toString());
    }
    
    protected boolean isEligibleForMergeOptimization() {
        return this.batchTargetTranslator.getTargetNode().isPartitionedTarget();
    }
    
    protected void executeMerge(final Set<String> changedPartitions, final Set<String> newPartitions) {
        final Dag<TaskNode> taskDag = (Dag<TaskNode>)new DagImpl();
        ProgressUtil.addToJobSummary(this.reqCtx.getJobId(), IWJob.JOB_TYPE.AWB_BATCH.name(), String.format("Merging rows to the main table for target: %s.", this.targetNode.getTableName()));
        int uniqueBuildId = 0;
        for (final String partition : changedPartitions) {
            final List<String> queries = this.batchTargetTranslator.generateMergeStatements(uniqueBuildId++, partition, this.schema);
            final List list;
            final Object o;
            IWRunTimeException runTimeException;
            taskDag.addDisconnectedNode((DagNode)new RunnableTaskNode(() -> {
                try {
                    this.jdbcExecutionSession.executeStatements(list);
                }
                catch (Exception e) {
                    this.LOGGER.error("Error while merging partition '{}'", o);
                    this.LOGGER.error("Exception {}", (Object)AwbUtil.getErrorMessage((Throwable)e));
                    runTimeException = new IWRunTimeException("{MERGE_RECORDS_ERROR}", (Throwable)e);
                    Throwables.propagate((Throwable)runTimeException);
                }
                return;
            }));
        }
        taskDag.makeAsEndNodes((Set)Sets.newHashSet((Object[])new TaskNode[] { (TaskNode)new RunnableTaskNode(() -> this.completeMerge(changedPartitions, newPartitions, this.schema)) }));
        new DefaultTaskDagExecutorImpl(this.reqCtx, AwbConfigs.getMergeExecPoolSize(), 0).execute((Dag)taskDag);
    }
    
    protected void executeAppend(final TargetNode targetNode) throws ExecutionSessionException, IOException {
        this.LOGGER.info("Location {}", (Object)targetNode.getCurrentTableLoc());
        final Location workingTableLocation = this.iwFileSystem.createLocation(targetNode.getCurrentTableLoc());
        this.executeDataBaseQuery(workingTableLocation);
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
    
    protected void executeIncremental(final TargetNode targetNode) throws ExecutionSessionException, IOException {
        final Location incrementalDeltaPath = this.iwFileSystem.createLocation(AwbPathUtil.getIncrementalDeltaStoragePath(targetNode));
        this.executeDataBaseQuery(incrementalDeltaPath);
    }
    
    protected SupportedExecutionEngines getSupportedEngines() {
        return new SupportedExecutionEngines((List)Arrays.asList(BatchEngine.HIVE, BatchEngine.SPARK), (List)Arrays.asList(this.reqCtx.getBatchEngine()));
    }
    
    private void completeMerge(final Set<String> changedPartitions, final Set<String> newPartitions, final Map<String, String> columnTypes) {
        final Location mainLocation = this.iwFileSystem.createLocation(this.targetNode.getCurrentTableLoc());
        final Location deltaLocation = this.iwFileSystem.createLocation(AwbPathUtil.getIncrementalDeltaStoragePath(this.targetNode));
        final Location mergedLocation = this.iwFileSystem.createLocation(AwbPathUtil.getIncrementalMergeStoragePath(this.targetNode));
        try {
            final List<String> dropMainTablesStmts = this.batchTargetTranslator.getDropMainTablesStmts();
            this.jdbcExecutionSession.executeStatements((List)dropMainTablesStmts);
            this.moveMergedPartitions(mainLocation, mergedLocation, (Set)changedPartitions);
            this.moveMergedPartitions(mainLocation, deltaLocation, (Set)newPartitions);
            final Map<String, Location> newPartitionSpec = this.batchTargetTranslator.getPrimaryPartitionSpec(newPartitions, columnTypes);
            final List<String> alterStmts = this.batchTargetTranslator.getAddPartitionStmts(newPartitionSpec);
            this.jdbcExecutionSession.executeStatements((List)alterStmts);
            if (AwbConfigs.isComputeStatsEnabled()) {
                final Map<String, Location> incrPartitionSpec = this.batchTargetTranslator.getPrimaryPartitionSpec(changedPartitions, columnTypes);
                incrPartitionSpec.putAll(newPartitionSpec);
                this.updateTableStats(incrPartitionSpec);
            }
            final List<String> dropMergeTablesStmts = this.batchTargetTranslator.getDropMergeTablesStmts();
            this.jdbcExecutionSession.executeStatements((List)dropMergeTablesStmts);
            this.hdfsUtils.deleteDirIfExists(deltaLocation.toString());
            this.hdfsUtils.deleteDirIfExists(mergedLocation.toString());
        }
        catch (Exception e) {
            this.LOGGER.error("Error while completing merge : {}", (Object)Throwables.getStackTraceAsString((Throwable)e));
            IWRunTimeException.propagate(e, "TARGET_MERGE_ERROR");
        }
    }
    
    private void updateTableStats(final Map<String, Location> incrPartitionSpec) throws ExecutionSessionException {
        final List<String> analyzeStmts = this.batchTargetTranslator.getUpdateStatsStmt(incrPartitionSpec);
        final Dag<TaskNode> taskDag = (Dag<TaskNode>)new DagImpl();
        for (final String analyzeStmt : analyzeStmts) {
            final String[] split;
            final String[] multiStmts;
            int length;
            int i = 0;
            String stmt;
            Exception ex = null;
            final TaskNode t = (TaskNode)new RunnableTaskNode(() -> {
                multiStmts = (split = analyzeStmt.split(";"));
                for (length = split.length; i < length; ++i) {
                    stmt = split[i];
                    try {
                        this.jdbcExecutionSession.executeStatement(stmt);
                    }
                    catch (Exception e) {
                        this.LOGGER.debug("Error while executing {}", (Object)stmt);
                        this.LOGGER.error("Exception {}", (Object)AwbUtil.getErrorMessage((Throwable)e));
                        if (e instanceof IWRunTimeException) {
                            ex = e;
                        }
                        else {
                            // new(merak.tools.ExceptionHandling.IWRunTimeException.class)
                            new IWRunTimeException("{UPDATE_TABLE_STATS_ERROR}", (Throwable)e);
                        }
                        Throwables.propagate((Throwable)ex);
                    }
                }
                return;
            });
            taskDag.addDisconnectedNode((DagNode)t);
        }
        new DefaultTaskDagExecutorImpl(this.reqCtx, AwbConfigs.getMergeExecPoolSize(), 0).execute((Dag)taskDag);
    }
    
    protected Map<String, String> getSchema() {
        Map<String, String> colNameTypes = (Map<String, String>)Maps.newHashMap();
        if (this.targetNode.isIntermediate() || !this.batchTargetTranslator.shouldCreateTable()) {
            this.LOGGER.debug("Task {} for node {} doesnt require column type extraction ", (Object)this.task.getId(), (Object)this.targetNode.getId());
            return colNameTypes;
        }
        final String schemaName = this.targetNode.getSchemaName();
        final String dummyTableName = "iw_schema_extract_" + this.targetNode.getTableName();
        String newType;
        final Function<String, String> typeOverWrite = (Function<String, String>)(type -> {
            newType = type;
            if (type.equals("void")) {
                newType = "String";
            }
            return newType;
        });
        colNameTypes = this.batchTargetTranslator.getSchema(this.reqCtx, schemaName, dummyTableName, typeOverWrite);
        return colNameTypes;
    }
    
    private Pattern getPrimaryPartitionRegex() {
        final StringBuffer primaryPartRegex = new StringBuffer();
        this.targetNode.getPrimaryPartitionColumns().forEach(col -> primaryPartRegex.append(String.format("%s=(.*)|", col.getGivenName())));
        return Pattern.compile(primaryPartRegex.toString());
    }
    
    private void createSecondaryPartitionDirectories(final Location parent) throws IOException {
        this.LOGGER.debug("Checking for secondary partitions at: {}", (Object)parent.toString());
        final Set<Location> directories = (Set<Location>)this.hdfsUtils.getChildDirectories(parent.getFullPath());
        final Set<Location> files = (Set<Location>)this.hdfsUtils.getChildFiles(parent.getFullPath());
        if (files.size() > 0) {
            this.LOGGER.debug("Found {} secondary partition files!", (Object)files.size());
            for (int i = 0; i < this.targetNode.getNumSecondaryPartitions(); ++i) {
                final Location partition = this.iwFileSystem.createLocation(parent.getFullPath(), String.format("%s%d", "ziw_", i));
                final String absolutePath = partition.getFullPathWithoutUri();
                long existingFileCount;
                if (directories.stream().anyMatch(d -> d.getFullPathWithoutUri().equals(absolutePath))) {
                    existingFileCount = this.hdfsUtils.getFileCountInDirectory(partition.getFullPath());
                    this.LOGGER.debug("Secondary partition folder {} already created and has {} files", (Object)partition.toString(), (Object)existingFileCount);
                }
                else {
                    if (!this.hdfsUtils.exists(partition.getFullPath())) {
                        this.LOGGER.debug("Creating secondary partition folder: {}", (Object)partition.toString());
                        Preconditions.checkState(this.hdfsUtils.mkdirs(partition.toString()), "Failed to create secondary partition folder %s!", new Object[] { partition.toString() });
                    }
                    existingFileCount = 0L;
                }
                final String filePrefix = String.format("%05d_", i);
                for (final Location file : files) {
                    final String fileName = file.getName();
                    if (SqlBatchQueryExecutor.BUCKET_NAME_PATTERN.matcher(fileName).matches() && i == Integer.parseInt(fileName.substring(0, fileName.indexOf(95)))) {
                        final String newFileName = filePrefix + existingFileCount++;
                        this.LOGGER.debug("Moving file {} to {}/{}", new Object[] { file, partition, newFileName });
                        final Location dest = this.iwFileSystem.createLocation(partition.toString(), newFileName);
                        this.hdfsUtils.moveFileToFile(file.getFullPath(), dest.getFullPath());
                    }
                }
            }
        }
        else if (directories.size() > 0) {
            this.LOGGER.debug("Found directories, checking for primary partitions.");
            final Pattern primaryPartitionRegex = this.getPrimaryPartitionRegex();
            for (final Location directory : directories) {
                if (primaryPartitionRegex.matcher(directory.getName()).matches()) {
                    this.createSecondaryPartitionDirectories(directory);
                }
            }
        }
        else {
            this.LOGGER.debug("No files or directories found!");
        }
    }
    
    private void executeDataBaseQuery(final Location workingTableLocation) throws ExecutionSessionException, IOException {
        this.jdbcExecutionSession.executeStatements(TranslatorUtils.getCreateDatabaseStatement(this.reqCtx, this.batchTargetTranslator.getSchemaName()));
        final List<String> tableStmts = this.batchTargetTranslator.generateTableStmts(this.schema);
        this.jdbcExecutionSession.executeStatements((List)tableStmts);
    }
    
    static {
        BUCKET_NAME_PATTERN = Pattern.compile("[0-9]+_[0-9]+");
    }
}
