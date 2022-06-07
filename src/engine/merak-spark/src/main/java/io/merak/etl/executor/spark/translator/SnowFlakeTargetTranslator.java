package io.merak.etl.executor.spark.translator;

import io.merak.etl.translator.*;
import io.merak.etl.executor.spark.task.*;
import io.merak.etl.executor.spark.translator.dto.*;

import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;
import org.apache.parquet.*;
import io.merak.etl.pipeline.dto.*;
import com.google.common.collect.*;
import com.google.common.base.*;

import io.merak.etl.sdk.task.*;

import java.util.*;

public class SnowFlakeTargetTranslator extends LoadTargetSparkTranslator
{
    private static String tableName;
    private static SaveMode saveMode;
    private static final String URL_KEY = "sfURL";
    private static final String ACCOUNT_KEY = "sfAccount";
    private static final String USER_KEY = "sfUser";
    private static final String PASSWORD_KEY = "sfPassword";
    private static final String WAREHOUSE_KEY = "sfWarehouse";
    private static final String STAGING_TABLE_KEY = "sfStagingTable";
    private static Map<String, String> sfCompatibleTypesMap;
    SnowFlakeTargetNode snowFlakeTargetNode;
    
    public SnowFlakeTargetTranslator(final SnowFlakeTargetNode node, final TranslatorContext translatorContext) {
        super((LoadTarget)node, translatorContext);
        this.snowFlakeTargetNode = (SnowFlakeTargetNode)this.node;
    }
    
    public static Map<String, String> getSfCompatibleTypesMap() {
        return SnowFlakeTargetTranslator.sfCompatibleTypesMap;
    }
    
    public static void setSfCompatibleTypesMap(final Map<String, String> sfCompatibleTypesMap) {
        SnowFlakeTargetTranslator.sfCompatibleTypesMap = sfCompatibleTypesMap;
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
    
    public static String getMergeTableStmt(final String databaseName, final String schemaName, final String tableName, final String stagingDatabaseName, final String stagingSchemaName, final String stagingTableName, final Map<String, String> columnMap, final List<Entity> naturalKeyColumns) {
        final StringBuilder builder = new StringBuilder();
        builder.append(String.format("MERGE INTO \"%s\".\"%s\".%s t1  USING \"%s\".\"%s\".%s t2 ON ", databaseName, schemaName, tableName, stagingDatabaseName, stagingSchemaName, stagingTableName));
        naturalKeyColumns.forEach(k -> builder.append(String.format("t1.%s = t2.%s AND ", k.getGivenName(), k.getGivenName())));
        builder.setLength(builder.length() - 4);
        builder.append("WHEN MATCHED THEN UPDATE SET ");
        columnMap.keySet().forEach(i -> builder.append(String.format("%s = t2.%s, ", i, i)));
        builder.setLength(builder.length() - 2);
        builder.append(" WHEN NOT MATCHED THEN INSERT (");
        builder.append(String.format("%s) ", String.join(",", columnMap.keySet())));
        builder.append(String.format("values (t2.%s)", String.join(", t2.", columnMap.keySet())));
        return builder.toString();
    }
    
    public static String getCreateDatabaseStmt(final String databaseName) {
        return String.format("CREATE DATABASE IF NOT EXISTS %s", databaseName);
    }
    
    public static String getCreateSchemaStmt(final String databaseName, final String schemaName) {
        return String.format("CREATE SCHEMA IF NOT EXISTS \"%s\".%s", databaseName, schemaName);
    }
    
    public static String getDropTableStmt(final String databaseName, final String schemaName, final String tableName) {
        return String.format("DROP TABLE  \"%s\".\"%s\".%s", databaseName, schemaName, tableName);
    }
    
    public static String getCompatibleDataType(final String dataType) {
        final String newDataType = SnowFlakeTargetTranslator.sfCompatibleTypesMap.get(dataType.trim().toLowerCase());
        return (newDataType != null) ? newDataType : dataType;
    }
    
    public static Properties getJdbcProperties(final SnowFlakeTargetNode node) {
        final Properties snowFlakeConnectionProp = node.getSnowFlakeConnectionProp();
        final Properties jdbcProperties = new Properties();
        jdbcProperties.put("user", snowFlakeConnectionProp.getProperty("sfUser"));
        jdbcProperties.put("account", snowFlakeConnectionProp.getProperty("sfAccount"));
        jdbcProperties.put("password", snowFlakeConnectionProp.getProperty("sfPassword"));
        jdbcProperties.put("warehouse", snowFlakeConnectionProp.getProperty("sfWarehouse"));
        if (!Strings.isNullOrEmpty(snowFlakeConnectionProp.getProperty("sfJdbcExtraParams"))) {
            jdbcProperties.put("connection_params", snowFlakeConnectionProp.getProperty("sfJdbcExtraParams"));
        }
        return jdbcProperties;
    }
    
    public void updateColumnDataTypes() {
        final String newDataType;
        this.snowFlakeTargetNode.getColumnMap().forEach((k, v) -> {
            newDataType = getCompatibleDataType(v);
            if (newDataType != v) {
                this.snowFlakeTargetNode.getColumnMap().put(k, newDataType);
            }
        });
    }
    
    private void preValidation() throws Exception {
        this.snowFlakeTargetNode.validate();
    }
    
    @Override
    protected void generateDataFrame() {
        final PipelineNode parent = this.getParentNode((PipelineNode)this.node);
        final DataFrameObject incomingDFO = this.sparkTranslatorState.getDataFrame(parent);
        final Dataset<Row> dataset = incomingDFO.getDataset();
        final List<String> plan = (List<String>)Lists.newArrayList();
        final Dataset<Row> sfTargetDF = this.select(dataset);
        plan.add(String.format("Node : %s", ((LoadTarget)this.node).getId()));
        this.snowFlakeTargetNode.setColumnMap((Map)getSchema(sfTargetDF));
        plan.add(String.format("Schema is %s", sfTargetDF));
        this.updateColumnDataTypes();
        plan.add(String.format("Updated column types", new Object[0]));
        try {
            this.preValidation();
        }
        catch (Exception e) {
            e.printStackTrace();
            Throwables.propagate((Throwable)e);
        }
        final DataFrameObject sfObject = new DataFrameObject(sfTargetDF, String.join("\n", plan));
        this.sparkTranslatorState.addDataFrame(this.node, sfObject);
    }
    
    @Override
    public TaskNode translate() {
        super.translate();
        return (TaskNode)new SparkSnowFlakeTaskNode(this, this.snowFlakeTargetNode, this.sparkTranslatorState);
    }
    
    public String getIfTableExistSql(final SnowFlakeTargetNode node) {
        final String sql = String.format("SELECT count(TABLE_NAME) FROM \"%s\".INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '%s' AND TABLE_NAME = '%s'", node.getDatabaseName(), node.getSchemaName(), node.getTableName());
        return sql;
    }
    
    public String getSFtableRowCountSql(final SnowFlakeTargetNode node) {
        final String sql = String.format("SELECT COUNT(*) FROM \"%s\".\"%s\".%s", node.getDatabaseName(), node.getSchemaName(), node.getTableName());
        return sql;
    }
    
    public List<String> getpreActionsSqlList(final LoadTarget.SyncMode buildMode) {
        final List<String> preActionsSqlList = new LinkedList<String>();
        SnowFlakeTargetTranslator.saveMode = SaveMode.Overwrite;
        if (LoadTarget.SyncMode.OVERWRITE == this.snowFlakeTargetNode.getSyncMode()) {
            preActionsSqlList.add(getCreateDatabaseStmt(this.snowFlakeTargetNode.getDatabaseName()));
            preActionsSqlList.add(getCreateSchemaStmt(this.snowFlakeTargetNode.getDatabaseName(), this.snowFlakeTargetNode.getSchemaName()));
            return preActionsSqlList;
        }
        if (LoadTarget.SyncMode.APPEND == this.snowFlakeTargetNode.getSyncMode()) {
            SnowFlakeTargetTranslator.saveMode = SaveMode.Append;
        }
        else if (LoadTarget.SyncMode.MERGE == this.snowFlakeTargetNode.getSyncMode()) {
            preActionsSqlList.add(getCreateDatabaseStmt(this.snowFlakeTargetNode.getDatabaseName()));
            preActionsSqlList.add(getCreateSchemaStmt(this.snowFlakeTargetNode.getDatabaseName(), this.snowFlakeTargetNode.getSchemaName()));
            return preActionsSqlList;
        }
        return preActionsSqlList;
    }
    
    static {
        (SnowFlakeTargetTranslator.sfCompatibleTypesMap = new HashMap<String, String>()).put("int", "decimal(38,0)");
        SnowFlakeTargetTranslator.sfCompatibleTypesMap.put("integer", "decimal(38,0)");
        SnowFlakeTargetTranslator.sfCompatibleTypesMap.put("bigint", "decimal(38,0)");
        SnowFlakeTargetTranslator.sfCompatibleTypesMap.put("smallint", "decimal(38,0)");
        SnowFlakeTargetTranslator.sfCompatibleTypesMap.put("tinyint", "decimal(38,0)");
    }
}
