package io.merak.etl.executor.spark.executor;

import io.merak.etl.executor.impl.*;
import io.merak.etl.sdk.task.*;
import io.merak.etl.context.*;
import io.merak.etl.utils.schema.*;
import io.merak.etl.executor.spark.session.*;
import io.merak.etl.executor.spark.task.*;
import io.merak.etl.utils.column.*;
import com.google.common.collect.*;
import io.merak.etl.translator.*;
import io.merak.etl.schema.*;
import java.util.*;
import io.merak.etl.utils.config.*;

import java.util.concurrent.atomic.*;
import com.google.common.base.*;
import org.apache.spark.sql.*;
import java.io.*;
import org.slf4j.*;

public class SparkCopySampleExecutor extends CopySampleExecutor
{
    private static final Logger LOGGER;
    private SparkSession spark;
    
    protected void preExecute(final Task taskNode, final RequestContext requestContext) {
        this.setup((SparkCopySampleTaskNode)taskNode, requestContext);
        this.refreshSourceTable();
    }
    
    protected void createSampleTable() {
        SparkCopySampleExecutor.LOGGER.info("Creating pipeline sample table schema");
        final String sourceSchema = this.sourceNode.getSchemaName();
        final String sourceTable = this.sourceNode.getTableName();
        final String pipelineSchema = AwbConfigs.getAWBWorkspaceSchema();
        final String pipelineTable = AwbConfigs.getInteractiveModeIntermediateTableName(this.requestContext.getPipelineId(), this.sourceNode.getId());
        List<TableColumn> columns = null;
        try {
            final BaseSchemaHandler schemaInstance = SchemaFactory.createSchemaInstance(this.requestContext);
            columns = (List<TableColumn>)schemaInstance.getTableSchema(this.requestContext, sourceSchema, sourceTable, true);
        }
        catch (Exception e) {
            SparkCopySampleExecutor.LOGGER.error("Connection could not be established to Spark Metastore");
            SparkCopySampleExecutor.LOGGER.error(e.getMessage());
        }
        StringBuilder columnNameAndTypes = new StringBuilder();
        if (columns != null) {
            for (final TableColumn tc : columns) {
                columnNameAndTypes.append(String.format("`%s` %s,", tc.getName(), tc.getType()));
            }
            columnNameAndTypes = columnNameAndTypes.deleteCharAt(columnNameAndTypes.length() - 1);
        }
        final List<String> stmts = (List<String>)Lists.newArrayList();
        try {
            this.executeQueries(TranslatorUtils.getCreateDatabaseStatement(this.requestContext, pipelineSchema));
        }
        catch (Exception e2) {
            SparkCopySampleExecutor.LOGGER.warn("error while creating database {}", (Object)e2.getMessage());
        }
        stmts.add(this.getDropTableStmt());
        stmts.add(String.format("CREATE EXTERNAL TABLE `%s`.`%s` (%s) STORED AS %s LOCATION '%s'", pipelineSchema, pipelineTable, columnNameAndTypes, AwbConfigs.getDefaultStorageFormat(), this.pipelineSampleLocation));
        this.executeQueries(stmts);
    }
    
    private void refreshSourceTable() {
        final String sourceSchema = this.sourceNode.getSchemaName();
        final String sourceTable = this.sourceNode.getTableName();
        final String refreshTable = String.format("REFRESH TABLE `%s`.`%s`", sourceSchema, sourceTable);
        final List<String> stmts = (List<String>)Lists.newArrayList();
        stmts.add(refreshTable);
        this.executeQueries(stmts);
    }
    
    public void setup(final SparkCopySampleTaskNode task, final RequestContext requestContext) {
        this.requestContext = requestContext;
        this.sourceNode = task.getSourceNode();
        this.pipelineSampleLocation = AwbPathUtil.getInteractiveIntermediateBasePath(requestContext.getPipelineId(), this.sourceNode.getId());
        this.spark = DFSparkSession.getInstance(requestContext).getSpark();
    }
    
    protected boolean isGlobalSampleInSync(final RequestContext requestContext, final String sourceSchema, final String sourceTable, final String globalSchema, final String globalSampleTable) {
        final AtomicBoolean isTableInSync = new AtomicBoolean(false);
        try {
            final BaseSchemaHandler schemaInstance = SchemaFactory.createSchemaInstance(requestContext);
            final List<TableColumn> sourceDescriptions = (List<TableColumn>)schemaInstance.getTableSchema(requestContext, sourceSchema, sourceTable, true);
            final List<TableColumn> globalSampleDescriptions = (List<TableColumn>)schemaInstance.getTableSchema(requestContext, globalSchema, globalSampleTable, true);
            if (sourceDescriptions.equals(globalSampleDescriptions)) {
                SparkCopySampleExecutor.LOGGER.info("Global sample is in sync with the source!");
                isTableInSync.set(true);
            }
            else {
                SparkCopySampleExecutor.LOGGER.info("Global sample is not in sync with the source!\nGlobal Sample: {}\nSource: {}", (Object)globalSampleDescriptions, (Object)sourceDescriptions);
            }
        }
        catch (Exception e) {
            SparkCopySampleExecutor.LOGGER.error("Error while trying to execute query");
            SparkCopySampleExecutor.LOGGER.info("Exception: {}", (Object)Throwables.getStackTraceAsString((Throwable)e));
        }
        return isTableInSync.get();
    }
    
    protected void executeQueries(final List<String> getSampleDataStatements) {
        if (getSampleDataStatements.isEmpty()) {
            return;
        }
        DFSparkSession.getInstance(this.requestContext).executeStatements(getSampleDataStatements);
    }
    
    protected String getDropTableStmt() {
        final String pipelineSchema = AwbConfigs.getAWBWorkspaceSchema();
        final String pipelineTable = AwbConfigs.getInteractiveModeIntermediateTableName(this.requestContext.getPipelineId(), this.sourceNode.getId());
        final String dropStmt = String.format("DROP TABLE IF EXISTS `%s`.`%s`", pipelineSchema, pipelineTable);
        return dropStmt;
    }
    
    protected void postSamplingExecution() {
        final String pipelineSchema = AwbConfigs.getAWBWorkspaceSchema();
        final String pipelineTable = AwbConfigs.getInteractiveModeIntermediateTableName(this.requestContext.getPipelineId(), this.sourceNode.getId());
        DFSparkSession.getInstance(this.requestContext).refreshTableCache(String.format("%s.%s", pipelineSchema, pipelineTable));
    }
    
    protected void copySampleData() throws IOException {
        SparkCopySampleExecutor.LOGGER.info("Copying sample data from global sample.");
        final String globalSchema = AwbConfigs.getAWBWorkspaceSchema();
        final String globalSampleTable = AwbConfigs.getGlobalSampleTableName(this.sourceNode.getTableId());
        final Dataset<Row> sourceDF = (Dataset<Row>)this.spark.sql(String.format("SELECT * FROM `%s`.`%s`", globalSchema, globalSampleTable));
        final String pipelineSchema = AwbConfigs.getAWBWorkspaceSchema();
        final String pipelineTable = AwbConfigs.getInteractiveModeIntermediateTableName(this.requestContext.getPipelineId(), this.sourceNode.getId());
        sourceDF.write().mode(SaveMode.Overwrite).format(AwbConfigs.getDefaultStorageFormat()).insertInto(String.format("`%s`.`%s`", pipelineSchema, pipelineTable));
    }
    
    static {
        LOGGER = LoggerFactory.getLogger((Class)SparkCopySampleExecutor.class);
    }
}
