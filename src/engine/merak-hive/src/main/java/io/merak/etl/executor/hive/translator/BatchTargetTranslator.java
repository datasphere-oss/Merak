package io.merak.etl.executor.hive.translator;

import java.util.stream.*;
import java.util.function.*;

import com.google.common.collect.*;
import com.google.common.base.*;
import org.apache.commons.lang.*;
import java.util.*;

import io.merak.adapters.filesystem.*;
import io.merak.etl.context.*;
import io.merak.etl.executor.hive.task.*;
import io.merak.etl.pipeline.dto.*;
import io.merak.etl.sdk.task.*;
import io.merak.etl.translator.*;
import io.merak.etl.utils.config.*;
import io.merak.etl.utils.exception.*;
import io.merak.etl.utils.shared.*;
import merak.tools.ExceptionHandling.*;

import java.sql.*;
import org.slf4j.*;

public abstract class BatchTargetTranslator extends SqlTargetTranslator
{
    private static Logger LOGGER;
    private static final String TMP_TAB_SUFFIX = "_iwtmp";
    protected String schemaName;
    protected String hdfsLocation;
    protected String schemaExtractTableName;
    protected String givenTableName;
    protected IWFileSystem iwFileSystem;
    protected String workingTableName;
    protected boolean usingTempTable;
    private TargetTranslatorUtils translatorUtils;
    
    public BatchTargetTranslator(final TargetNode node, final TranslatorContext translatorContext) {
        super(node, translatorContext);
        this.translatorUtils = new TargetTranslatorUtils(node);
        final String targetLocation = node.getHdfsLocation();
        this.iwFileSystem = IWFileSystem.getFileSystemFromPath(targetLocation);
    }
    
    protected void end() {
        this.builder.append(")\n");
    }
    
    public TaskNode translate() {
        this.builder.insert(0, "WITH\n");
        if (!this.injectAuditColumns()) {
            this.start(this.node.getId()).select((PipelineNode)this.node).from(this.parentNodeName).end();
        }
        this.schemaName = this.node.getSchemaName();
        this.givenTableName = this.node.getTableName();
        this.workingTableName = this.givenTableName + "_iwtmp";
        this.usingTempTable = (this.node.isOverwriteMode() && !this.node.isIntermediate());
        this.hdfsLocation = this.node.getCurrentTableLoc();
        if (this.node.isMergeMode()) {
            this.givenTableName = "iwdf_delta_" + this.givenTableName;
            this.hdfsLocation = AwbPathUtil.getIncrementalDeltaStoragePath(this.node);
        }
        this.schemaExtractTableName = "iw_schema_extract_" + this.node.getTableName();
        return (TaskNode)new BatchQueryTaskNode(this, this.node, this.builder.toString());
    }
    
    public String getSchemaExtractViewDropStmt() {
        final String dropSchemaViewStmt = String.format("DROP VIEW IF EXISTS `%s`.`%s`", this.schemaName, this.schemaExtractTableName);
        BatchTargetTranslator.LOGGER.debug("Stmt for dropping schema extract table {}", (Object)dropSchemaViewStmt);
        return dropSchemaViewStmt;
    }
    
    public String getSchemaName() {
        return this.schemaName;
    }
    
    public List<String> getSchemaExtractTableCreationStmts() {
        final List<String> stmts = (List<String>)Lists.newArrayList();
        stmts.add(this.getSchemaExtractViewDropStmt());
        final String dummyTableCreate = String.format("CREATE VIEW `%s`.`%s` AS %s SELECT * FROM %s LIMIT 0", this.schemaName, this.schemaExtractTableName, this.builder.toString(), this.node.getId());
        BatchTargetTranslator.LOGGER.debug("Creating dummy schema extract table {}", (Object)dummyTableCreate);
        stmts.add(dummyTableCreate);
        return stmts;
    }
    
    public String getSchemaExtractTableDescribeStmt() {
        final String describeTable = String.format("DESCRIBE `%s`.`%s`", this.schemaName, this.schemaExtractTableName);
        BatchTargetTranslator.LOGGER.debug("Describing dummy schema extract table {}", (Object)describeTable);
        return describeTable;
    }
    
    public boolean shouldCreateTable() {
        return !this.node.isAppendMode();
    }
    
    public String getStmtForTest() {
        this.builder.append(String.format("SELECT * FROM %s", this.node.getId()));
        return this.builder.toString();
    }
    
    public List<String> generateTableStmts(final Map<String, String> columnTypes) {
        final String tableName = this.usingTempTable ? this.workingTableName : this.givenTableName;
        final String location = this.usingTempTable ? this.node.getFutureTableLoc() : this.hdfsLocation;
        final List<String> stmts = (List<String>)Lists.newArrayList();
        if (this.shouldCreateTable()) {
            stmts.add(String.format("DROP TABLE IF EXISTS `%s`.`%s` PURGE", this.schemaName, tableName));
        }
        if (this.node.isIntermediate()) {
            this.builder.append(String.format("SELECT * FROM %s", this.node.getId()));
            this.builder.insert(0, String.format("CREATE TABLE `%s`.`%s`\n STORED AS %s\n LOCATION '%s' AS\n", this.node.getSchemaName(), this.node.getTableName(), this.node.getStorageFormat(), this.node.getHdfsLocation()));
            stmts.add(this.builder.toString());
        }
        else {
            if (this.shouldCreateTable()) {
                stmts.add(this.getCreateTableStmt(columnTypes, tableName, location));
            }
            stmts.add(this.getInsertStmt(tableName));
        }
        return stmts;
    }
    
    protected abstract String getCreateTableStmt(final Map<String, String> p0, final String p1, final String p2);
    
    private String getInsertStmt(final String tableName) {
        final StringBuilder insertStmt = new StringBuilder();
        insertStmt.append((CharSequence)this.builder);
        insertStmt.append(String.format("INSERT INTO `%s`.`%s`", this.schemaName, tableName));
        this.appendPartitionInsertClause(insertStmt);
        final String sequence = this.getGivenColumnNames();
        BatchTargetTranslator.LOGGER.info("the sequence of column is {}", (Object)sequence);
        insertStmt.append(String.format("\nSELECT %s FROM %s", sequence, this.node.getId()));
        return insertStmt.toString();
    }
    
    protected String getGivenColumnNames() {
        return (String)this.node.getOutputEntities().stream().map(i -> String.format("`%s`", i.getGivenName())).collect(Collectors.joining(","));
    }
    
    protected void appendColumns(final Map<String, String> columnTypes, final StringBuilder createStmt) {
        createStmt.append("\n(");
        final List<String> colStmts = (List<String>)Lists.newArrayList();
        final List<String> list;
        this.node.getOutputEntities().forEach(c -> {
            if (!this.node.getPrimaryPartitionColumns().contains(c)) {
                list.add(c.getOutputName((PipelineNode)this.node) + " " + columnTypes.get(c.getGivenName()));
            }
            return;
        });
        createStmt.append(String.join(",\n", colStmts));
        createStmt.append("\n)");
    }
    
    protected void appendPartitionCreateClause(final Map<String, String> columnTypes, final StringBuilder createStmt) {
        final List<String> colStmts = (List<String>)Lists.newArrayList();
        colStmts.clear();
        final String outName;
        final String givenName;
        final List<String> list;
        this.node.getPrimaryPartitionColumns().forEach(pc -> {
            outName = this.getOutputNameForProperty(pc);
            givenName = outName.substring(1, outName.length() - 1);
            list.add(outName + " " + columnTypes.get(givenName));
            return;
        });
        createStmt.append(String.format("\nPARTITIONED BY ( %s )", String.join(", ", colStmts)));
    }
    
    protected void appendPartitionInsertClause(final StringBuilder insertStmt) {
        final String partitions = (String)this.node.getPrimaryPartitionColumns().stream().map(c -> this.getOutputNameForProperty(c)).collect(Collectors.joining(","));
        if (!partitions.isEmpty()) {
            insertStmt.append(String.format(" PARTITION (%s)", partitions));
        }
    }
    
    protected String getNaturalKeyColumnNames() {
        String naturalKeyColumnNames = "ZIW_ROW_ID";
        if (!this.node.getNaturalKeyColumns().isEmpty()) {
            if (this.node.isPartitionedTarget() && this.node.getPrimaryPartitionColumns().stream().anyMatch(c -> this.node.getNaturalKeyColumns().stream().anyMatch(key -> key.equals((Object)c)))) {
                return naturalKeyColumnNames;
            }
            naturalKeyColumnNames = (String)this.node.getNaturalKeyColumns().stream().map(this::getOutputNameForProperty).collect(Collectors.joining(", "));
        }
        return naturalKeyColumnNames;
    }
    
    protected String getBucketingColumnNames() {
        final String bucketingColumnNames = "ZIW_ROW_ID";
        List<Entity> bucketColumns = (List<Entity>)Lists.newArrayList((Iterable)this.node.getSecondaryPartitionColumns());
        if (bucketColumns.isEmpty()) {
            bucketColumns = (List<Entity>)Lists.newArrayList((Iterable)this.node.getNaturalKeyColumns());
        }
        final List<Entity> finalBucketColumns = bucketColumns;
        if (this.node.isPartitionedTarget() && this.node.getPrimaryPartitionColumns().stream().anyMatch(c -> finalBucketColumns.stream().anyMatch(key -> key.equals((Object)c)))) {
            return bucketingColumnNames;
        }
        return finalBucketColumns.stream().map((Function<? super Object, ?>)this::getOutputNameForProperty).collect((Collector<? super Object, ?, String>)Collectors.joining(", "));
    }
    
    public List<String> generateMergeStatements(final int uniqueBuildId, final String partition, final Map<String, String> columnTypes) {
        final List<String> statements = (List<String>)Lists.newLinkedList();
        BatchTargetTranslator.LOGGER.debug("Generating Merge Statements for partition: {} using id: {}", (Object)partition, (Object)uniqueBuildId);
        BatchTargetTranslator.LOGGER.info("node additive information is {}", (Object)this.node.getSchemaChanged());
        if (this.node.getSchemaChanged()) {
            final ExecutionMetaDataUtils metaDataUtils = new ExecutionMetaDataUtils();
            final List<Entity> entities = (List<Entity>)metaDataUtils.getResetOutputEntities(this.node, this.requestContext);
            this.node.setOrderedOutputEntities((List)entities);
        }
        final String mainTable = this.addCreateMergeTableStmt(uniqueBuildId, "main", this.node.getCurrentTableLoc(), partition, columnTypes, statements);
        final String deltaTable = this.addCreateMergeTableStmt(uniqueBuildId, "delta", AwbPathUtil.getIncrementalDeltaStoragePath(this.node), partition, columnTypes, statements);
        final String mergedTable = this.addCreateMergeTableStmt(uniqueBuildId, "merged", AwbPathUtil.getIncrementalMergeStoragePath(this.node), partition, columnTypes, statements);
        final StringBuilder builder = new StringBuilder();
        if (!this.node.isSCD2()) {
            builder.append(String.format("INSERT OVERWRITE TABLE `%s`.`%s` ", this.schemaName, mergedTable));
        }
        this.appendMergeStatement(builder, mainTable, deltaTable, mergedTable);
        statements.add(builder.toString());
        return statements;
    }
    
    protected abstract String addCreateMergeTableStmt(final int p0, final String p1, final String p2, final String p3, final Map<String, String> p4, final List<String> p5);
    
    public Map<String, Location> getPrimaryPartitionSpec(final Set<String> partitionPaths, final Map<String, String> columnTypes) {
        final Map<String, Location> changedPrimarySpec = (Map<String, Location>)Maps.newHashMap();
        for (final String partitionPath : partitionPaths) {
            final String[] partitionHierarchy = partitionPath.split("/");
            if (partitionHierarchy.length <= 1) {
                continue;
            }
            final List<String> partitionValues = (List<String>)Lists.newLinkedList();
            int i = 0;
            while (i < partitionHierarchy.length - 1) {
                final String[] columnValue = partitionHierarchy[i].split("=");
                Preconditions.checkState(columnValue.length == 2, "Invalid partition spec: %s", new Object[] { partitionHierarchy[i] });
                final String name = columnValue[0];
                final String value = StringEscapeUtils.escapeJavaScript(AwbPathUtil.unescapePathName(columnValue[1]));
                if (value.equals("__HIVE_DEFAULT_PARTITION__")) {
                    if (AwbConfigs.failOnNullOrEmptyPartitionValue()) {
                        throw new RuntimeException("Table is partitioned and has null or empty values");
                    }
                    BatchTargetTranslator.LOGGER.error("Skipping partition {}, has null or empty values!", (Object)partitionPath);
                    break;
                }
                else {
                    partitionValues.add(String.format("`%s`='%s'", name, value));
                    ++i;
                }
            }
            if (i < partitionHierarchy.length - 1) {
                continue;
            }
            final Location destination = this.iwFileSystem.createLocation(this.node.getCurrentTableLoc(), partitionPath);
            changedPrimarySpec.put(String.join(", ", partitionValues), destination);
        }
        return changedPrimarySpec;
    }
    
    protected String addPartition(final String tableName, final String partition, final Location location) {
        return String.format("ALTER TABLE %s ADD IF NOT EXISTS PARTITION (%s) LOCATION '%s'", tableName, partition, location.toString());
    }
    
    public List<String> switchTableStmts(final boolean purge) {
        final List<String> stmts = (List<String>)Lists.newArrayList();
        final String purgeStmt = purge ? " PURGE" : "";
        stmts.add(String.format("DROP TABLE IF EXISTS `%s`.`%s`%s", this.schemaName, this.givenTableName, purgeStmt));
        stmts.add(String.format("ALTER TABLE `%s`.`%s` RENAME TO `%s`.`%s`", this.schemaName, this.workingTableName, this.schemaName, this.givenTableName));
        return stmts;
    }
    
    public abstract List<String> getDropMergeTablesStmts();
    
    public abstract List<String> getDropMainTablesStmts();
    
    public abstract List<String> getAddPartitionStmts(final Map<String, Location> p0);
    
    public abstract List<String> getUpdateStatsStmt(final Map<String, Location> p0);
    
    public abstract String getCreateStatsStmt();
    
    public Map<String, String> getSchema(final RequestContext reqCtx, final String schemaName, final String tableName, final Function<String, String> typeOverwrite) {
        final Map<String, String> colNameTypes = (Map<String, String>)Maps.newHashMap();
        try {
            BatchTargetTranslator.LOGGER.debug("Checking out shared connection");
            SharedConnection.execute(reqCtx, reqCtx.getPipelineId(), sharedConn -> {
                Statement stmt;
                final Throwable t2;
                final Function<List<String>, Boolean> executeStmts = (Function<List<String>, Boolean>)(extractTableCreationStmts -> {
                    extractTableCreationStmts.forEach(sqlStmt -> {
                        BatchTargetTranslator.LOGGER.debug("Executing {}", (Object)sqlStmt);
                        try {
                            stmt = sharedConn.createStatement();
                            try {
                                stmt.execute(sqlStmt);
                            }
                            catch (Throwable t) {
                                throw t;
                            }
                            finally {
                                if (stmt != null) {
                                    if (t2 != null) {
                                        try {
                                            stmt.close();
                                        }
                                        catch (Throwable t3) {
                                            t2.addSuppressed(t3);
                                        }
                                    }
                                    else {
                                        stmt.close();
                                    }
                                }
                            }
                        }
                        catch (Exception e) {
                            BatchTargetTranslator.LOGGER.error("Exception {}", (Object)AwbUtil.getErrorMessage((Throwable)e));
                            IWRunTimeException.propagate(e, "HIVE_BATCH_EXECUTION_ERROR");
                        }
                        return;
                    });
                    return Boolean.valueOf(true);
                });
                try {
                    executeStmts.apply(TranslatorUtils.getCreateDatabaseStatement(reqCtx, this.getSchemaName()));
                }
                catch (Exception e2) {
                    BatchTargetTranslator.LOGGER.warn("failed while creating database {}", (Object)e2.getMessage());
                }
                final List<String> extractTableCreationStmts2 = this.getSchemaExtractTableCreationStmts();
                executeStmts.apply(extractTableCreationStmts2);
                try (final Statement stmt2 = sharedConn.createStatement()) {
                    final String describeTable = this.getSchemaExtractTableDescribeStmt();
                    BatchTargetTranslator.LOGGER.debug("Executing describing schema extract table {}", (Object)describeTable);
                    try (final ResultSet describeRS = stmt2.executeQuery(describeTable)) {
                        while (describeRS.next()) {
                            final String type = describeRS.getString(2);
                            final String newType = typeOverwrite.apply(type);
                            BatchTargetTranslator.LOGGER.info("inside describe column name is {} type is {} newType {}", new Object[] { describeRS.getString(1), type, newType });
                            colNameTypes.put(describeRS.getString(1), newType);
                        }
                    }
                }
                final String dropTableStmt = this.getSchemaExtractViewDropStmt();
                BatchTargetTranslator.LOGGER.debug("Dropping schema extract table {}", (Object)dropTableStmt);
                try (final Statement stmt3 = sharedConn.createStatement()) {
                    stmt3.execute(dropTableStmt);
                }
            });
        }
        catch (Exception e) {
            BatchTargetTranslator.LOGGER.error("Error while trying to extract schema for {}.{}", (Object)schemaName, (Object)tableName);
            BatchTargetTranslator.LOGGER.error("Exception {}", (Object)AwbUtil.getErrorMessage((Throwable)e));
            IWRunTimeException.propagate(e, "HIVE_BATCH_EXECUTION_ERROR");
        }
        return colNameTypes;
    }
    
    public RequestContext getRequestContext() {
        return this.requestContext;
    }
    
    static {
        BatchTargetTranslator.LOGGER = LoggerFactory.getLogger((Class)BatchTargetTranslator.class);
    }
}
