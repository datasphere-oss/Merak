package io.merak.etl.executor.hive.translator;

import java.util.stream.*;
import merak.tools.utils.*;
import io.merak.adapters.filesystem.*;
import io.merak.etl.pipeline.dto.*;
import io.merak.etl.translator.*;
import io.merak.etl.utils.config.*;

import com.google.common.collect.*;
import com.google.common.base.*;
import org.apache.commons.lang.*;

import java.util.*;

import org.slf4j.*;

public class ImpalaBatchTargetTranslator extends BatchTargetTranslator
{
    private static Logger LOGGER;
    protected List<String> dropMergeTableStmts;
    protected List<String> dropMainTableStmts;
    
    public ImpalaBatchTargetTranslator(final TargetNode node, final TranslatorContext translatorContext) {
        super(node, translatorContext);
        this.dropMergeTableStmts = (List<String>)Lists.newArrayList();
        this.dropMainTableStmts = (List<String>)Lists.newArrayList();
    }
    
    public boolean isPartitionedTarget() {
        return !this.node.isIntermediate();
    }
    
    @Override
    protected String getCreateTableStmt(final Map<String, String> columnTypes, final String tableName, final String location) {
        final StringBuilder createStmt = new StringBuilder();
        createStmt.append(String.format("CREATE EXTERNAL TABLE `%s`.`%s`", this.schemaName, tableName));
        this.appendColumns(columnTypes, createStmt);
        if (this.node.hasPrimaryPartitions() || this.node.getNumSecondaryPartitions() > 0) {
            this.appendPartitionCreateClause(columnTypes, createStmt);
        }
        createStmt.append(String.format("\nSTORED AS %s\nLOCATION '%s'", this.node.getStorageFormat(), location));
        return createStmt.toString();
    }
    
    @Override
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
        colStmts.add(String.format("`%s` %s", "ziw_sec_p", columnTypes.get("ziw_sec_p")));
        createStmt.append(String.format("\nPARTITIONED BY ( %s )", String.join(", ", colStmts)));
    }
    
    @Override
    protected String getGivenColumnNames() {
        final List<String> columnNamesList = (List<String>)this.node.getOutputEntities().stream().map(i -> String.format("`%s`", i.getGivenName())).collect(Collectors.toList());
        columnNamesList.add(String.format("`%s`", "ziw_sec_p"));
        return columnNamesList.stream().collect((Collector<? super Object, ?, String>)Collectors.joining(","));
    }
    
    @Override
    protected void appendPartitionInsertClause(final StringBuilder insertStmt) {
        final List<String> partitions = (List<String>)Lists.newArrayList();
        this.node.getPrimaryPartitionColumns().forEach(c -> partitions.add(this.getOutputNameForProperty(c)));
        partitions.add(String.format("`%s`", "ziw_sec_p"));
        insertStmt.append(String.format(" PARTITION (%s)", String.join(",", partitions)));
    }
    
    @Override
    protected String addCreateMergeTableStmt(final int uniqueBuildId, final String tableSuffix, final String baseLocation, final String partition, final Map<String, String> columnTypes, final List<String> statements) {
        final StringBuilder builder = new StringBuilder();
        final String tableName = String.format("%s_%d_%s", this.node.getTableName(), uniqueBuildId, tableSuffix);
        final String tableLocation = String.format("%s%s", IWPathUtil.addSeparatorIfNotExists(baseLocation), partition);
        builder.append(String.format("CREATE EXTERNAL TABLE `%s`.`%s`", this.schemaName, tableName));
        this.appendColumns(columnTypes, builder);
        builder.append(String.format("\nSTORED AS %s\nLOCATION '%s'", this.node.getStorageFormat(), tableLocation));
        statements.add(builder.toString());
        if (tableSuffix.equalsIgnoreCase("main")) {
            this.dropMainTableStmts.add(String.format("ALTER TABLE `%s`.`%s` SET TBLPROPERTIES('EXTERNAL'='FALSE')", this.schemaName, tableName));
            this.dropMainTableStmts.add(String.format("DROP TABLE `%s`.`%s` PURGE", this.schemaName, tableName));
        }
        else {
            this.dropMergeTableStmts.add(String.format("ALTER TABLE `%s`.`%s` SET TBLPROPERTIES('EXTERNAL'='FALSE')", this.schemaName, tableName));
            this.dropMergeTableStmts.add(String.format("DROP TABLE `%s`.`%s` PURGE", this.schemaName, tableName));
        }
        return tableName;
    }
    
    @Override
    public Map<String, Location> getPrimaryPartitionSpec(final Set<String> partitionPaths, final Map<String, String> columnTypes) {
        final Map<String, Location> changedPrimarySpec = (Map<String, Location>)Maps.newHashMap();
        final Set<String> stringTypeColumns = (Set<String>)Sets.newHashSet();
        final Set<String> set;
        columnTypes.forEach((columnName, columnType) -> {
            if (ImpalaStringTypes.exists(columnType)) {
                set.add(columnName);
            }
            return;
        });
        int i = 0;
        for (final String partitionPath : partitionPaths) {
            final List<String> partitionValues = (List<String>)Lists.newLinkedList();
            final String[] split;
            final String[] partitionHierarchy = split = partitionPath.split("/");
            final int length = split.length;
            int j = 0;
            while (j < length) {
                final String partition = split[j];
                final String[] columnValue = partition.split("=");
                Preconditions.checkState(columnValue.length == 2, "Invalid partition spec: %s", new Object[] { partition });
                final String name = columnValue[0];
                final String value = StringEscapeUtils.escapeJavaScript(AwbPathUtil.unescapePathName(columnValue[1]));
                if (value.equals("__HIVE_DEFAULT_PARTITION__")) {
                    if (AwbConfigs.failOnNullOrEmptyPartitionValue()) {
                        throw new RuntimeException("Table is partitioned and has null or empty values");
                    }
                    ImpalaBatchTargetTranslator.LOGGER.error("Skipping partition {}, has null or empty values!", (Object)partitionPath);
                    break;
                }
                else {
                    if (stringTypeColumns.contains(name)) {
                        partitionValues.add(String.format("`%s`='%s'", name, value));
                    }
                    else {
                        partitionValues.add(String.format("`%s`=%s", name, value));
                    }
                    ++i;
                    ++j;
                }
            }
            if (i >= partitionHierarchy.length) {
                final Location destination = this.iwFileSystem.createLocation(this.node.getCurrentTableLoc(), partitionPath);
                changedPrimarySpec.put(String.join(", ", partitionValues), destination);
            }
        }
        return changedPrimarySpec;
    }
    
    @Override
    public List<String> getDropMergeTablesStmts() {
        return this.dropMergeTableStmts;
    }
    
    @Override
    public List<String> getDropMainTablesStmts() {
        return this.dropMainTableStmts;
    }
    
    @Override
    public List<String> getAddPartitionStmts(final Map<String, Location> newPartitionSpec) {
        final String tableName = String.format("`%s`.`%s`", this.node.getSchemaName(), this.node.getTableName());
        final List<String> alterStmts = (List<String>)Lists.newLinkedList();
        newPartitionSpec.entrySet().forEach(i -> alterStmts.add(this.addPartition(tableName, i.getKey(), (Location)i.getValue())));
        alterStmts.add(String.format("REFRESH %s", tableName));
        return alterStmts;
    }
    
    @Override
    public List<String> getUpdateStatsStmt(final Map<String, Location> incrPartitionSpec) {
        final List<String> analyzeStmts = (List<String>)Lists.newLinkedList();
        final String tableName = String.format("`%s`.`%s`", this.node.getSchemaName(), this.node.getTableName());
        if (!AwbConfigs.isImpalaIncrementalStatsEnabled()) {
            analyzeStmts.add(String.format("DROP STATS %s;COMPUTE STATS %s", tableName, tableName));
            return analyzeStmts;
        }
        final List<String> list;
        final Object o;
        incrPartitionSpec.entrySet().forEach(i -> list.add(String.format("DROP INCREMENTAL STATS %s PARTITION (%s);COMPUTE INCREMENTAL STATS %s PARTITION (%s)", o, i.getKey(), o, i.getKey())));
        return analyzeStmts;
    }
    
    @Override
    public String getCreateStatsStmt() {
        String incremental = "";
        if (AwbConfigs.isImpalaIncrementalStatsEnabled()) {
            incremental = " INCREMENTAL";
        }
        return String.format("COMPUTE%s STATS `%s`.`%s`", incremental, this.node.getSchemaName(), this.node.getTableName());
    }
    
    @Override
    public List<String> switchTableStmts(final boolean purge) {
        final List<String> stmts = super.switchTableStmts(true);
        stmts.add(String.format("ALTER TABLE `%s`.`%s` SET TBLPROPERTIES('EXTERNAL'='FALSE')", this.schemaName, this.givenTableName));
        return stmts;
    }
    
    static {
        ImpalaBatchTargetTranslator.LOGGER = LoggerFactory.getLogger((Class)ImpalaBatchTargetTranslator.class);
    }
    
    private enum ImpalaStringTypes
    {
        VARCHAR, 
        STRING, 
        CHAR;
        
        public static boolean exists(final String type) {
            return "STRING".equalsIgnoreCase(type) || "VARCHAR".equalsIgnoreCase(type) || "CHAR".equalsIgnoreCase(type);
        }
    }
}
