package io.merak.etl.executor.spark.translator;

import io.merak.etl.sdk.task.*;

import java.util.stream.*;

import org.apache.spark.sql.*;

import io.merak.etl.context.*;
import io.merak.etl.translator.*;
import com.google.common.collect.*;
import org.apache.spark.sql.types.*;
import io.merak.etl.utils.constants.*;
import io.merak.etl.executor.spark.metadata.*;
import io.merak.etl.executor.spark.task.*;
import io.merak.etl.executor.spark.translator.dto.*;
import io.merak.adapters.filesystem.*;
import com.google.common.base.*;
import io.merak.etl.utils.config.*;
import org.apache.commons.lang.*;
import java.util.*;
import io.merak.etl.pipeline.dto.*;

public class BatchTargetTranslator extends SparkTargetTranslator
{
    protected IWFileSystem iwFileSystem;
    
    public BatchTargetTranslator(final TargetNode node, final TranslatorContext translatorContext) {
        super(node, translatorContext);
        this.LOGGER.trace("Target properties {} ", (Object)node.getProperties());
        this.LOGGER.info("hdfs location: {}", (Object)node.getHdfsLocation());
        this.iwFileSystem = IWFileSystem.getFileSystemFromPath(node.getHdfsLocation());
    }
    
    public TranslatorContext getTranslatorContext() {
        return this.translatorContext;
    }
    
    @Override
    public TaskNode translate() {
        super.translate();
        return (TaskNode)new SparkBatchTaskNode(this, (TargetNode)this.node, this.sparkTranslatorState);
    }
    
    @Override
    protected void generateDataFrame() {
        final Dataset<Row> inputDF = this.getDataset(this.parentNode);
        final List<String> plan = (List<String>)Lists.newArrayList();
        plan.add(String.format("Node : %s", ((TargetNode)this.node).getId()));
        final List<String> selectCols = (List<String>)((TargetNode)this.node).getOutputEntities().stream().map(i -> String.format("%s AS %s", i.getInputName(), i.getOutputName(i.getOwnerNode()))).collect(Collectors.toList());
        this.LOGGER.debug("Target input columns :{} ", (Object)String.join(",", (CharSequence[])inputDF.columns()));
        final Dataset<Row> targetDF = this.select(inputDF);
        this.LOGGER.debug("Target output columns :{} ", (Object)String.join(",", (CharSequence[])targetDF.columns()));
        plan.add(String.format("Columns :%s", String.join(",", selectCols)));
        final DataFrameObject targetDFO = new DataFrameObject(targetDF, String.join("\n", plan));
        this.sparkTranslatorState.addDataFrame(this.node, targetDFO);
    }
    
    public DataFrameWriter overWriteNonHiveCompatible(final String tableName, final String targetLoc) {
        DataFrameWriter dataFrameWriter = this.getWriterNonCompatible((TargetNode)this.node, this.getDf());
        this.LOGGER.info("over write table name is {} and schema is {}", (Object)tableName, (Object)this.getDf().schema().toString());
        this.LOGGER.debug(" overwrite mode creating table {} at location {}", (Object)tableName, (Object)targetLoc);
        dataFrameWriter = dataFrameWriter.format(((TargetNode)this.node).getStorageFormat()).option("path", targetLoc);
        return dataFrameWriter;
    }
    
    public DataFrameWriter mergeNonHiveCompatible(final String tableName, final String targetLoc) {
        DataFrameWriter dataFrameWriter = this.getWriterNonCompatible((TargetNode)this.node, this.getDf());
        this.LOGGER.info("over write table name is {} and schema is {}", (Object)tableName, (Object)this.getDf().schema().toString());
        this.LOGGER.debug(" overwrite mode creating table {} at location {}", (Object)tableName, (Object)targetLoc);
        dataFrameWriter = dataFrameWriter.format(((TargetNode)this.node).getStorageFormat()).option("path", targetLoc);
        return dataFrameWriter;
    }
    
    public List<String> overWriteHiveCompatible(final String tableName, final String targetLoc) {
        return this.generateTableStatement(tableName, targetLoc);
    }
    
    public List<String> mergeHiveCompatible(final String tableName, final String targetLoc) {
        return this.generateTableStatement(tableName, targetLoc);
    }
    
    public void switchTable(final String workingTable, final String actualTable) {
        final String dropTable = String.format("DROP TABLE IF EXISTS %s", actualTable);
        final String renameTable = String.format("ALTER TABLE %s RENAME TO %s", workingTable, actualTable);
        this.LOGGER.debug("Drop table sttmt {}", (Object)dropTable);
        this.LOGGER.debug("Rename table sttmt {}", (Object)renameTable);
        this.spark.sql(dropTable);
        this.spark.sql(renameTable);
        this.LOGGER.debug("successfully altered the table");
    }
    
    public Dataset<Row> getDf() {
        return this.sparkTranslatorState.getDataFrame(this.node).getDataset();
    }
    
    public StructType getSchemaForTarget() {
        return SparkMetaDataUtils.getSchemaForTarget(this.sparkTranslatorState, this.node);
    }
    
    public String getStorageFormat() {
        return ((TargetNode)this.node).getStorageFormat();
    }
    
    public boolean shouldCreateTable() {
        return !((TargetNode)this.node).isAppendMode() || ((TargetNode)this.node).getSchemaChanged();
    }
    
    public List<String> generateDatabaseStatement(final RequestContext requestContext, final String schemaName) {
        return (List<String>)TranslatorUtils.getCreateDatabaseStatement(requestContext, schemaName);
    }
    
    private List<String> generateTableStatement(final String tableName, final String targetLoc) {
        this.LOGGER.debug("Generating table statements " + tableName + "  Location:" + targetLoc);
        final List<String> stmts = (List<String>)Lists.newArrayList();
        if (this.shouldCreateTable()) {
            stmts.add(String.format("DROP TABLE IF EXISTS %s", tableName));
            this.LOGGER.debug("Extracting schema for target table..");
            final Map<String, String> columnTypes = this.getColumnsFromSchema();
            stmts.add(this.getCreateTableStmt(columnTypes, tableName, targetLoc));
        }
        return stmts;
    }
    
    public List<String> generateDatabaseStmts(final String tableName) {
        final List<String> stmts = (List<String>)Lists.newArrayList();
        stmts.addAll(TranslatorUtils.getCreateDatabaseStatement(this.requestContext, ((TargetNode)this.node).getSchemaName()));
        stmts.add(String.format("DROP TABLE IF EXISTS %s", tableName));
        return stmts;
    }
    
    public Map<String, String> getColumnsFromSchema() {
        final Map<String, String> columnTypes = (Map<String, String>)Maps.newHashMap();
        final StructType schema = this.getSchemaForTarget();
        final StructField[] fields2;
        final StructField[] fields = fields2 = schema.fields();
        for (final StructField f : fields2) {
            final DataType dataType = (f.dataType() == DataTypes.NullType) ? DataTypes.StringType : f.dataType();
            columnTypes.put(f.name(), dataType.simpleString());
        }
        return columnTypes;
    }
    
    private String getCreateTableStmt(final Map<String, String> columnTypes, final String tableName, final String targetLoc) {
        this.LOGGER.debug("Generating create table statement" + tableName + "  Location:" + targetLoc);
        final StringBuilder createStmt = new StringBuilder();
        final String external = String.format("%s ", ((TargetNode)this.node).isIntermediate() ? "" : "EXTERNAL");
        createStmt.append(String.format("CREATE %sTABLE %s", external, tableName));
        this.appendColumns(columnTypes, createStmt);
        if (((TargetNode)this.node).hasPrimaryPartitions() || ((TargetNode)this.node).isHiveCompatible()) {
            this.appendPartitionCreateClause(columnTypes, createStmt);
        }
        createStmt.append(String.format("STORED AS %s\nLOCATION '%s'", ((TargetNode)this.node).getStorageFormat(), targetLoc));
        if (this.hasTableProperties()) {
            createStmt.append(this.createTableProperties());
        }
        this.LOGGER.debug("Create Table Statements : " + (Object)createStmt);
        return createStmt.toString();
    }
    
    protected boolean hasTableProperties() {
        return (((TargetNode)this.node).getIndexKeyColumns().size() > 0 && ((TargetNode)this.node).getStorageFormat().equals(AwbConstants.FileFormat.ORC.name())) || ((TargetNode)this.node).getCompressionFormat() != null;
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
        return ((TargetNode)this.node).getIndexKeyColumns().size() > 0;
    }
    
    protected String getIndexStmt() {
        final StringBuilder indexStmt = new StringBuilder();
        try {
            if (((TargetNode)this.node).getStorageFormat().equals(AwbConstants.FileFormat.ORC.name()) && ((TargetNode)this.node).getIndexKeyColumns().size() > 0) {
                this.LOGGER.debug("creating Index on columns {}", ((TargetNode)this.node).getIndexKeyColumns().stream().map(column -> column.getOutputName((PipelineNode)this.node)).collect(Collectors.toList()));
                indexStmt.append(String.format("'orc.bloom.filter.columns'='%s'", ((TargetNode)this.node).getIndexKeyColumns().stream().map(column -> column.getOutputName((PipelineNode)this.node)).collect(Collectors.joining(", "))));
            }
        }
        catch (Throwable t) {
            this.LOGGER.error("Unable to add indexes", (Object)Throwables.getStackTraceAsString(t));
            Throwables.propagate(t);
        }
        return indexStmt.toString();
    }
    
    protected boolean hasCompression() {
        return ((TargetNode)this.node).getCompressionFormat() != null;
    }
    
    protected String getCompressionStmt() {
        final StringBuilder compressionStmt = new StringBuilder();
        try {
            if (((TargetNode)this.node).getStorageFormat() != null) {
                compressionStmt.append(String.format("'%s.compress'='%s'", ((TargetNode)this.node).getStorageFormat().toLowerCase(), ((TargetNode)this.node).getCompressionFormat()));
            }
        }
        catch (Throwable t) {
            this.LOGGER.error("Unable to add compression", (Object)Throwables.getStackTraceAsString(t));
            Throwables.propagate(t);
        }
        return compressionStmt.toString();
    }
    
    private void appendPartitionCreateClause(final Map<String, String> columnTypes, final StringBuilder createStmt) {
        final List<String> colStmts = (List<String>)Lists.newArrayList();
        colStmts.clear();
        ((TargetNode)this.node).getPrimaryPartitionColumns().forEach(pc -> colStmts.add(pc.getOutputName((PipelineNode)this.node) + " " + columnTypes.get(pc.getGivenName())));
        if (((TargetNode)this.node).isHiveCompatible() || ((TargetNode)this.node).hasPrimaryPartitions()) {
            colStmts.add(String.format("`%s` %s", "ziw_sec_p", columnTypes.get("ziw_sec_p")));
        }
        createStmt.append(String.format("\nPARTITIONED BY ( %s )", String.join(", ", colStmts)));
    }
    
    public List<String> getUpdateStatsStmt(final Map<String, Location> incrPartitionSpec) {
        final List<String> analyzeStmts = (List<String>)Lists.newLinkedList();
        final String tableName = String.format("`%s`.`%s`", ((TargetNode)this.node).getSchemaName(), ((TargetNode)this.node).getTableName());
        if (AwbConfigs.hiveAnalyzeWorks()) {
            if (((TargetNode)this.node).hasPrimaryPartitions() || ((TargetNode)this.node).isHiveCompatible()) {
                incrPartitionSpec.entrySet().forEach(i -> analyzeStmts.add(String.format("ANALYZE TABLE %s PARTITION (%s) COMPUTE STATISTICS NOSCAN", tableName, i.getKey())));
            }
            else {
                analyzeStmts.add(String.format("ANALYZE TABLE %s COMPUTE STATISTICS NOSCAN", tableName));
            }
        }
        return analyzeStmts;
    }
    
    private void appendColumns(final Map<String, String> columnTypes, final StringBuilder createStmt) {
        createStmt.append("\n(");
        final List<String> colStmts = (List<String>)Lists.newArrayList();
        final List<String> colNames = (List<String>)Lists.newArrayList();
        final List<String> list;
        final List<String> list2;
        ((TargetNode)this.node).getOutputEntities().forEach(c -> {
            list.add(c.getGivenName());
            if (!((TargetNode)this.node).getPrimaryPartitionColumns().contains(c)) {
                if (!c.getOutputName((PipelineNode)this.node).equals(String.format("`%s`", "ziw_sec_p"))) {
                    list2.add(c.getOutputName((PipelineNode)this.node) + " " + columnTypes.get(c.getGivenName()));
                }
            }
            return;
        });
        this.LOGGER.debug("Append column outputEntities {}", (Object)colNames);
        createStmt.append(String.join(",\n", colStmts));
        createStmt.append("\n)");
    }
    
    public String getCountQuery() {
        return String.format("select count(*) from `%s`.`%s`", ((TargetNode)this.node).getSchemaName(), ((TargetNode)this.node).getTableName());
    }
    
    public Map<String, Location> getPrimaryPartitionSpec(final Set<String> partitionPaths, final Map<String, String> columnTypes) {
        final Map<String, Location> changedPrimarySpec = (Map<String, Location>)Maps.newHashMap();
        for (final String partitionPath : partitionPaths) {
            final String[] partitionHierarchy = partitionPath.split("/");
            final List<String> partitionValues = (List<String>)Lists.newLinkedList();
            int i;
            for (i = 0; i < partitionHierarchy.length; ++i) {
                final String[] columnValue = partitionHierarchy[i].split("=");
                Preconditions.checkState(columnValue.length == 2, "Invalid partition spec: %s", new Object[] { partitionHierarchy[i] });
                final String name = columnValue[0];
                final String value = StringEscapeUtils.escapeJavaScript(AwbPathUtil.unescapePathName(columnValue[1]));
                partitionValues.add(String.format("`%s`='%s'", name, value));
            }
            if (i >= partitionHierarchy.length) {
                final Location destination = this.iwFileSystem.createLocation(((TargetNode)this.node).getCurrentTableLoc(), partitionPath);
                changedPrimarySpec.put(String.join(", ", partitionValues), destination);
            }
        }
        return changedPrimarySpec;
    }
    
    public List<String> getAddPartitionStmts(final Map<String, Location> newPartitionSpec) {
        final String tableName = String.format("`%s`.`%s`", ((TargetNode)this.node).getSchemaName(), ((TargetNode)this.node).getTableName());
        final List<String> alterStmts = (List<String>)Lists.newArrayList();
        if (!AwbConfigs.hiveAnalyzeWorks()) {
            alterStmts.add(String.format("ALTER TABLE %s SET TBLPROPERTIES ('COLUMN_STATS_ACCURATE'='false')", tableName));
        }
        if (((TargetNode)this.node).hasPrimaryPartitions() || ((TargetNode)this.node).isHiveCompatible()) {
            newPartitionSpec.entrySet().forEach(partitionSpec -> alterStmts.add(this.addPartition(tableName, partitionSpec.getKey(), (Location)partitionSpec.getValue())));
        }
        return alterStmts;
    }
    
    protected String addPartition(final String tableName, final String partition, final Location path) {
        return String.format("ALTER TABLE %s ADD IF NOT EXISTS PARTITION (%s) LOCATION '%s'", tableName, partition, path.toString());
    }
    
    public String getUpdatedRecordEndTimetamp(final String scd2Granularity) {
        final String currentTimeStamp = "cast ('" + this.getCurrentTimestamp() + "' as timestamp )";
        final String intervalExpression = this.getIntervalExp(currentTimeStamp, scd2Granularity.toLowerCase());
        final String lowerCase = scd2Granularity.toLowerCase();
        String subExp = null;
        switch (lowerCase) {
            case "year": {
                subExp = String.format("concat(year(%s),'-','12','-','31',' ','23',':','59',':','59')", intervalExpression);
                break;
            }
            case "month": {
                subExp = String.format("concat(year(%s),'-',month(%s),'-','28',' ','23',':','59',':','59')", intervalExpression, intervalExpression);
                break;
            }
            case "day": {
                subExp = String.format("concat(year(%s),'-',month(%s),'-',day(%s),' ','23',':','59',':','59')", intervalExpression, intervalExpression, intervalExpression);
                break;
            }
            case "hour": {
                subExp = String.format("concat(year(%s),'-',month(%s),'-',day(%s),' ',hour(%s),':','59',':','59')", intervalExpression, intervalExpression, intervalExpression, intervalExpression);
                break;
            }
            case "minute": {
                subExp = String.format("concat(year(%s),'-',month(%s),'-',day(%s),' ',hour(%s),':',minute(%s),':','59')", intervalExpression, intervalExpression, intervalExpression, intervalExpression, intervalExpression);
                break;
            }
            default: {
                subExp = currentTimeStamp + " - interval '1' second";
                break;
            }
        }
        final String columnExp = String.format("CAST (unix_timestamp(%s,'yyyy-MM-dd HH:mm:ss') AS TIMESTAMP )", subExp);
        return columnExp;
    }
    
    private String getIntervalExp(final String currentTimestamp, final String scd2Granularity) {
        return String.format("%s - interval '1' %s", currentTimestamp, scd2Granularity);
    }
    
    public DataFrameWriter getNonHiveCompatibleWriter(final TargetNode node, final Dataset<Row> dataset) {
        return this.getWriterNonCompatible(node, dataset);
    }
}
