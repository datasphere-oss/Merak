package io.merak.etl.executor.spark.translator;

import io.merak.etl.translator.*;
import io.merak.etl.executor.spark.exceptions.*;
import io.merak.etl.executor.spark.task.*;
import io.merak.etl.executor.spark.translator.dto.*;

import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;

import com.google.common.collect.*;
import com.google.common.base.*;

import java.util.stream.*;
import io.merak.etl.sdk.task.*;

import java.util.*;
import io.merak.etl.pipeline.dto.*;

public class AzureDWTargetTranslator extends LoadTargetSparkTranslator
{
    private static String tableName;
    private static SaveMode saveMode;
    public static final String DW_URL_KEY = "dwURL";
    public static final String DW_ACCOUNT_KEY = "dwAccount";
    public static final String DW_ACCESS_KEY = "dwAccess";
    public static final String DW_USER_KEY = "dwUser";
    public static final String DW_PASSWORD_KEY = "dwPassword";
    public static final String DW_WAREHOUSE_KEY = "dwWarehouse";
    public static final String DW_BLOB_CONTAINER_KEY = "dwcontainer";
    public static final String DW_STAGING_TABLE_KEY = "dwStagingTable";
    public static final String DW_JDBC_EXTRA_PARAMS = "dwJdbcExtraParams";
    public static final String DW_SCHEMA_KEY = "dwSchema";
    AzureDWNode azureDWNode;
    
    public AzureDWTargetTranslator(final AzureDWNode node, final TranslatorContext translatorContext) {
        super((LoadTarget)node, translatorContext);
        this.azureDWNode = (AzureDWNode)this.node;
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
    
    public static String getRenameTableStmt(final String schemaName, final String tableName, final String changed_TableName) {
        return String.format("RENAME OBJECT %s.%s TO %s", schemaName, tableName, changed_TableName);
    }
    
    public static String getCreateSchemaStmt(final String schemaName) {
        return String.format("IF NOT EXISTS ( SELECT  * FROM  sys.schemas WHERE   name = N'%s' ) EXEC('CREATE SCHEMA %s') ", schemaName, schemaName);
    }
    
    public static String getDropTableStmt(final String schemaName, final String tableName) {
        return String.format("DROP TABLE  \"%s\".%s", schemaName, tableName);
    }
    
    private void preValidation() throws AzureDWExecutionException {
        try {
            this.azureDWNode.validate();
        }
        catch (Exception e) {
            throw new AzureDWExecutionException(e);
        }
    }
    
    @Override
    protected void generateDataFrame() {
        final PipelineNode parent = this.getParentNode((PipelineNode)this.node);
        final DataFrameObject incomingDFO = this.sparkTranslatorState.getDataFrame(parent);
        final Dataset<Row> dataSet = incomingDFO.getDataset();
        final List<String> plan = (List<String>)Lists.newArrayList();
        final Dataset<Row> azureDWTargetDF = this.select(dataSet);
        plan.add(String.format("Node : %s", ((LoadTarget)this.node).getId()));
        this.azureDWNode.setColumnMap((Map)getSchema(azureDWTargetDF));
        plan.add(String.format("Schema is %s", azureDWTargetDF));
        try {
            this.preValidation();
        }
        catch (AzureDWExecutionException e) {
            e.printStackTrace();
            Throwables.propagate((Throwable)e);
        }
        final DataFrameObject azureDWObject = new DataFrameObject(azureDWTargetDF, String.join("\n", plan));
        this.sparkTranslatorState.addDataFrame(this.node, azureDWObject);
    }
    
    @Override
    protected Map<String, String> getAuditColumns() {
        final Map<String, String> auditColumns = (Map<String, String>)Maps.newLinkedHashMap();
        auditColumns.put("ZIW_ROW_ID".toLowerCase(), this.getRowIdExpr());
        return auditColumns;
    }
    
    public String getRowIdExpr() {
        return (this.azureDWNode.getNaturalKeyColumns().size() == 0) ? "uuid()" : String.format("hex(concat_ws(\":##:\", %s ))", this.azureDWNode.getNaturalKeyColumns().stream().map(column -> String.format("nvl(cast(%s as string),\"NULL\")", column.getGivenName())).collect(Collectors.joining(", ")));
    }
    
    @Override
    public TaskNode translate() {
        super.translate();
        return (TaskNode)new SparkAzureDWTaskNode(this, this.azureDWNode, this.sparkTranslatorState);
    }
    
    public String getIfTableExistSql(final AzureDWNode node) {
        return String.format("SELECT count(TABLE_NAME) AS TABLE_COUNT FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '%s' AND TABLE_NAME = '%s'", node.getSchemaName(), node.getTableName());
    }
    
    public String getAzureDWtableRowCountSql(final AzureDWNode node) {
        final String sql = String.format("SELECT COUNT(*) AS ROW_COUNT FROM \"%s\".%s", node.getSchemaName(), node.getTableName());
        return sql;
    }
    
    public List<String> getpreActionsSqlList(final LoadTarget.SyncMode buildMode) {
        final List<String> preActionsSqlList = new LinkedList<String>();
        AzureDWTargetTranslator.saveMode = SaveMode.Overwrite;
        if (LoadTarget.SyncMode.OVERWRITE == this.azureDWNode.getSyncMode()) {
            preActionsSqlList.add(getCreateSchemaStmt(this.azureDWNode.getSchemaName()));
            return preActionsSqlList;
        }
        if (LoadTarget.SyncMode.APPEND == this.azureDWNode.getSyncMode()) {
            AzureDWTargetTranslator.saveMode = SaveMode.Append;
        }
        else if (LoadTarget.SyncMode.MERGE == this.azureDWNode.getSyncMode()) {
            preActionsSqlList.add(getCreateSchemaStmt(this.azureDWNode.getSchemaName()));
            return preActionsSqlList;
        }
        return preActionsSqlList;
    }
    
    public List<String> getpostActionsSqlList(final LoadTarget.SyncMode buildMode) {
        final List<String> postActionsSqlList = new LinkedList<String>();
        if (LoadTarget.SyncMode.MERGE == this.azureDWNode.getSyncMode()) {
            final String table_name = this.azureDWNode.getTableName();
            postActionsSqlList.add(getRenameTableStmt(this.azureDWNode.getSchemaName(), this.azureDWNode.getTableName(), this.azureDWNode.getTableName() + "_TEMP_OLD"));
            postActionsSqlList.add(getRenameTableStmt(this.azureDWNode.getSchemaName(), this.azureDWNode.getStagingTableName(), table_name));
            postActionsSqlList.add(getDropTableStmt(this.azureDWNode.getSchemaName(), this.azureDWNode.getTableName() + "_TEMP_OLD"));
        }
        return postActionsSqlList;
    }
}
