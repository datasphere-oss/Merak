package io.merak.etl.executor.hive.executor.impl;

import com.google.common.collect.*;

import io.merak.etl.context.*;
import io.merak.etl.executor.impl.*;
import io.merak.etl.schema.*;
import io.merak.etl.sdk.task.*;
import io.merak.etl.task.impl.*;
import io.merak.etl.translator.*;
import io.merak.etl.utils.column.*;
import io.merak.etl.utils.config.*;
import io.merak.etl.utils.schema.*;
import io.merak.etl.utils.shared.*;

import java.util.*;
import java.util.concurrent.atomic.*;

import com.google.common.base.*;
import java.sql.*;

import org.slf4j.*;

public class SqlCopySampleExecutor extends CopySampleExecutor
{
    private static final Logger LOGGER;
    
    public void setup(final CopySampleTaskNode task, final RequestContext requestContext) {
        this.requestContext = requestContext;
        this.sourceNode = task.getSourceNode();
        this.pipelineSampleLocation = AwbPathUtil.getInteractiveIntermediateBasePath(requestContext.getPipelineId(), this.sourceNode.getId());
    }
    
    protected void preExecute(final Task taskNode, final RequestContext requestContext) {
        this.setup((CopySampleTaskNode)taskNode, requestContext);
    }
    
    protected void createSampleTable() {
        SqlCopySampleExecutor.LOGGER.info("Creating pipeline sample table schema");
        final String sourceSchema = this.sourceNode.getSchemaName();
        final String sourceTable = this.sourceNode.getTableName();
        final String pipelineSchema = AwbConfigs.getAWBWorkspaceSchema();
        final String pipelineTable = AwbConfigs.getInteractiveModeIntermediateTableName(this.requestContext.getPipelineId(), this.sourceNode.getId());
        List<TableColumn> columns = null;
        try {
            columns = (List<TableColumn>)SchemaFactory.createSchemaInstance(this.requestContext).getTableSchema(this.requestContext, sourceSchema, sourceTable, true);
        }
        catch (Exception e) {
            SqlCopySampleExecutor.LOGGER.error("Connection could not be established to Hive Metastore");
            SqlCopySampleExecutor.LOGGER.error(e.getMessage());
        }
        StringBuilder columnNameAndTypes = new StringBuilder();
        if (columns != null && columns.size() > 0) {
            for (final TableColumn tc : columns) {
                columnNameAndTypes.append(String.format("`%s` %s,", tc.getName(), tc.getType()));
            }
            columnNameAndTypes = columnNameAndTypes.deleteCharAt(columnNameAndTypes.length() - 1);
        }
        final List<String> stmts = (List<String>)Lists.newArrayList();
        this.executeQueries(TranslatorUtils.getCreateDatabaseStatement(this.requestContext, pipelineSchema));
        stmts.add(this.getDropTableStmt());
        stmts.add(String.format("CREATE EXTERNAL TABLE `%s`.`%s` (%s) STORED AS %s LOCATION '%s'", pipelineSchema, pipelineTable, columnNameAndTypes.toString(), AwbConfigs.getDefaultStorageFormat(), this.pipelineSampleLocation));
        this.executeQueries(stmts);
    }
    
    protected boolean isGlobalSampleInSync(final RequestContext requestContext, final String sourceSchema, final String sourceTable, final String globalSchema, final String globalSampleTable) {
        final AtomicBoolean isTableInSync = new AtomicBoolean(false);
        final String poolingKey = requestContext.getPoolingCriteria();
        try {
            SharedConnection.execute(requestContext, poolingKey, connection -> {
                final BaseSchemaHandler schemaInstance = SchemaFactory.createSchemaInstance(requestContext);
                final List<TableColumn> sourceDescriptions = (List<TableColumn>)schemaInstance.getTableSchema(requestContext, sourceSchema, sourceTable, true);
                final List<TableColumn> globalSampleDescriptions = (List<TableColumn>)schemaInstance.getTableSchema(requestContext, globalSchema, globalSampleTable, true);
                if (sourceDescriptions.equals(globalSampleDescriptions)) {
                    isTableInSync.set(true);
                }
                else {
                    SqlCopySampleExecutor.LOGGER.info("Global sample is not in sync with the source!\nGlobal Sample: {}\nSource: {}", (Object)globalSampleDescriptions, (Object)sourceDescriptions);
                }
            });
        }
        catch (Exception e) {
            SqlCopySampleExecutor.LOGGER.error("Error while trying to execute query");
            SqlCopySampleExecutor.LOGGER.info("Exception: {}", (Object)Throwables.getStackTraceAsString((Throwable)e));
        }
        return isTableInSync.get();
    }
    
    protected void executeQueries(final List<String> getSampleDataStatements) {
        try {
            SharedConnection.execute(this.requestContext, (List)getSampleDataStatements);
        }
        catch (Exception e) {
            SqlCopySampleExecutor.LOGGER.error("Error while trying to execute query");
            SqlCopySampleExecutor.LOGGER.trace("Exception: {}", (Object)Throwables.getStackTraceAsString((Throwable)e));
            Throwables.propagate((Throwable)e);
        }
    }
    
    protected String getDropTableStmt() {
        final String pipelineSchema = AwbConfigs.getAWBWorkspaceSchema();
        final String pipelineTable = AwbConfigs.getInteractiveModeIntermediateTableName(this.requestContext.getPipelineId(), this.sourceNode.getId());
        final String dropStmt = String.format("DROP TABLE IF EXISTS `%s`.`%s` PURGE", pipelineSchema, pipelineTable);
        return dropStmt;
    }
    
    protected void postSamplingExecution() {
    }
    
    private void refreshSampleTable() {
        SqlCopySampleExecutor.LOGGER.info("Refreshing pipeline sample table in Impala");
        final String pipelineSchema = AwbConfigs.getAWBWorkspaceSchema();
        final String pipelineTable = AwbConfigs.getInteractiveModeIntermediateTableName(this.requestContext.getPipelineId(), this.sourceNode.getId());
        final List<String> stmts = (List<String>)Lists.newArrayList();
        stmts.add(String.format("REFRESH `%s`.%s", pipelineSchema, pipelineTable));
        this.executeQueries(stmts);
    }
    
    static {
        LOGGER = LoggerFactory.getLogger((Class)SqlCopySampleExecutor.class);
    }
}
