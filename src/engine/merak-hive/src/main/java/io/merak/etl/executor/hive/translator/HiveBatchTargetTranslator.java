package io.merak.etl.executor.hive.translator;

import com.google.common.collect.*;
import java.util.*;
import java.util.stream.*;

import com.google.common.base.*;
import merak.tools.utils.*;
import io.merak.adapters.filesystem.*;
import io.merak.etl.pipeline.dto.*;
import io.merak.etl.translator.*;
import io.merak.etl.utils.config.*;
import io.merak.etl.utils.constants.*;
import io.merak.metadata.impl.dto.tables.configuration.*;

import org.slf4j.*;

public class HiveBatchTargetTranslator extends BatchTargetTranslator
{
    private static Logger LOGGER;
    
    public HiveBatchTargetTranslator(final TargetNode node, final TranslatorContext translatorContext) {
        super(node, translatorContext);
    }
    
    @Override
    public List<String> getSchemaExtractTableCreationStmts() {
        final List<String> stmts = (List<String>)Lists.newArrayList();
        stmts.add("SET hive.auto.convert.join=true");
        stmts.addAll(super.getSchemaExtractTableCreationStmts());
        stmts.add("SET hive.auto.convert.join=false");
        return stmts;
    }
    
    @Override
    protected String getCreateTableStmt(final Map<String, String> columnTypes, final String tableName, final String location) {
        final StringBuilder createStmt = new StringBuilder();
        final String external = String.format("%s ", this.node.isIntermediate() ? "" : "EXTERNAL");
        createStmt.append(String.format("CREATE %sTABLE `%s`.`%s`", external, this.schemaName, tableName));
        this.appendColumns(columnTypes, createStmt);
        if (this.node.hasPrimaryPartitions()) {
            this.appendPartitionCreateClause(columnTypes, createStmt);
        }
        this.appendSecondaryPartitionClause(createStmt);
        createStmt.append(String.format("STORED AS %s\nLOCATION '%s'", this.node.getStorageFormat(), location));
        if (this.hasTableProperties()) {
            createStmt.append(this.createTableProperties());
        }
        HiveBatchTargetTranslator.LOGGER.info("get create table statement is {}", (Object)createStmt.toString());
        return createStmt.toString();
    }
    
    protected boolean hasTableProperties() {
        return this.node.getIndexKeyColumns().size() > 0 || this.node.getCompressionFormat() != null;
    }
    
    protected String createTableProperties() {
        final StringBuilder tablePropertiesStmt = new StringBuilder();
        tablePropertiesStmt.append("\nTBLPROPERTIES (\n");
        final List<String> tblPropertStmt = (List<String>)Lists.newArrayList();
        if (this.shouldCreateIndex()) {
            tblPropertStmt.add(this.getIndexStmt());
        }
        if (this.hasCompression()) {
            tblPropertStmt.add(this.getCompressionStmt());
        }
        tablePropertiesStmt.append(tblPropertStmt.stream().collect((Collector<? super Object, ?, String>)Collectors.joining(",\n ")));
        tablePropertiesStmt.append("\n)");
        return tablePropertiesStmt.toString();
    }
    
    protected boolean shouldCreateIndex() {
        return this.node.getIndexKeyColumns().size() > 0;
    }
    
    protected String getIndexStmt() {
        final StringBuilder indexStmt = new StringBuilder();
        try {
            if (this.node.getStorageFormat().equals(AwbConstants.FileFormat.ORC.name()) && this.node.getIndexKeyColumns().size() > 0) {
                HiveBatchTargetTranslator.LOGGER.debug("creating Index on columns {}", this.node.getIndexKeyColumns().stream().map(column -> column.getOutputName((PipelineNode)this.node)).collect(Collectors.toList()));
                indexStmt.append(String.format("'orc.bloom.filter.columns'='%s'", this.node.getIndexKeyColumns().stream().map(column -> column.getOutputName((PipelineNode)this.node)).collect(Collectors.joining(", "))));
            }
        }
        catch (Throwable t) {
            HiveBatchTargetTranslator.LOGGER.error("Unable to add indexes", (Object)Throwables.getStackTraceAsString(t));
            Throwables.propagate(t);
        }
        return indexStmt.toString();
    }
    
    protected boolean hasCompression() {
        return this.node.getCompressionFormat() != null;
    }
    
    protected String getCompressionStmt() {
        final StringBuilder compressionStmt = new StringBuilder();
        try {
            if (this.node.getStorageFormat() != null) {
                compressionStmt.append(String.format("'%s.compress'='%s'", this.node.getStorageFormat().toLowerCase(), this.node.getCompressionFormat()));
            }
        }
        catch (Throwable t) {
            HiveBatchTargetTranslator.LOGGER.error("Unable to add compression", (Object)Throwables.getStackTraceAsString(t));
            Throwables.propagate(t);
        }
        return compressionStmt.toString();
    }
    
    protected void appendSecondaryPartitionClause(final StringBuilder createStmt) {
        String sortedBy = "";
        if (!this.node.getSortBy().isEmpty()) {
            final String colOrders = String.join(", ", (Iterable<? extends CharSequence>)this.node.getSortBy().stream().map(s -> this.getOutputNameForProperty(this.node.getOutputEntityByName(s.getColumnName())) + " " + s.getOrderType()).collect(Collectors.toList()));
            sortedBy = "SORTED BY (" + colOrders + ") ";
        }
        final String bucketingColumnNames = this.getBucketingColumnNames();
        createStmt.append(String.format("\nCLUSTERED BY (%s) %sINTO %s BUCKETS\n", bucketingColumnNames, sortedBy, this.node.getNumSecondaryPartitions()));
    }
    
    @Override
    protected String addCreateMergeTableStmt(final int uniqueBuildId, final String tableSuffix, final String baseLocation, final String partition, final Map<String, String> columnTypes, final List<String> statements) {
        final StringBuilder builder = new StringBuilder();
        final String tableName = String.format("%s_%d_%s", this.node.getTableName(), uniqueBuildId, tableSuffix);
        final String tableLocation = String.format("%s%s", IWPathUtil.addSeparatorIfNotExists(baseLocation), partition);
        builder.append(String.format("CREATE TEMPORARY EXTERNAL TABLE `%s`.`%s`", this.schemaName, tableName));
        this.appendColumns(columnTypes, builder);
        builder.append(String.format("\nSTORED AS %s\nLOCATION '%s'", this.node.getStorageFormat(), tableLocation));
        statements.add(builder.toString());
        return tableName;
    }
    
    @Override
    public List<String> getDropMergeTablesStmts() {
        return (List<String>)Lists.newArrayList();
    }
    
    @Override
    public List<String> getDropMainTablesStmts() {
        return (List<String>)Lists.newArrayList();
    }
    
    @Override
    public List<String> getAddPartitionStmts(final Map<String, Location> newPartitionSpec) {
        final String tableName = String.format("`%s`.`%s`", this.node.getSchemaName(), this.node.getTableName());
        final List<String> alterStmts = (List<String>)Lists.newArrayList();
        if (!AwbConfigs.hiveAnalyzeWorks()) {
            alterStmts.add(String.format("ALTER TABLE %s SET TBLPROPERTIES ('COLUMN_STATS_ACCURATE'='false')", tableName));
        }
        if (this.node.hasPrimaryPartitions()) {
            newPartitionSpec.entrySet().forEach(partitionSpec -> alterStmts.add(this.addPartition(tableName, partitionSpec.getKey(), (Location)partitionSpec.getValue())));
        }
        return alterStmts;
    }
    
    @Override
    public List<String> getUpdateStatsStmt(final Map<String, Location> incrPartitionSpec) {
        final List<String> analyzeStmts = (List<String>)Lists.newLinkedList();
        final String tableName = String.format("`%s`.`%s`", this.node.getSchemaName(), this.node.getTableName());
        if (AwbConfigs.hiveAnalyzeWorks()) {
            if (this.node.hasPrimaryPartitions()) {
                incrPartitionSpec.entrySet().forEach(i -> analyzeStmts.add(String.format("ANALYZE TABLE %s PARTITION (%s) COMPUTE STATISTICS", tableName, i.getKey())));
            }
            else {
                analyzeStmts.add(String.format("ANALYZE TABLE %s COMPUTE STATISTICS", tableName));
            }
        }
        return analyzeStmts;
    }
    
    @Override
    public String getCreateStatsStmt() {
        String analyzeStmts = "";
        if (AwbConfigs.hiveAnalyzeWorks()) {
            final StringBuilder partitionClause = new StringBuilder("");
            this.appendPartitionInsertClause(partitionClause);
            analyzeStmts = String.format("ANALYZE TABLE `%s`.`%s`%s COMPUTE STATISTICS", this.schemaName, this.node.getTableName(), partitionClause.toString());
        }
        return analyzeStmts;
    }
    
    static {
        HiveBatchTargetTranslator.LOGGER = LoggerFactory.getLogger((Class)HiveBatchTargetTranslator.class);
    }
}
