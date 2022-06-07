package io.merak.etl.executor.hive.executor.impl;

import io.merak.etl.context.*;
import io.merak.etl.executor.hive.metadata.*;
import io.merak.etl.executor.hive.task.*;
import io.merak.etl.executor.impl.*;
import io.merak.etl.schema.*;
import io.merak.etl.sdk.executor.*;
import io.merak.etl.sdk.task.*;
import io.merak.etl.utils.column.*;
import io.merak.etl.utils.config.*;
import io.merak.etl.utils.schema.*;
import io.merak.etl.utils.shared.*;
import merak.tools.ExceptionHandling.*;
import java.sql.*;

import com.google.common.base.*;

import java.util.*;

import org.slf4j.*;

public class HiveSchemaExecutor implements TaskExecutor
{
    private static final Logger LOGGER;
    
    public void execute(final Task taskNode, final TaskExecutorContext ctx) {
        final RequestContext reqCtx = (RequestContext)ctx.getContext();
        final HiveSchemaTaskNode task = (HiveSchemaTaskNode)taskNode;
        final String createOutputViewQuery = task.getCreateOutputViewQuery();
        final String dropOutputViewQuery = task.getDropOutputViewQuery();
        try {
            SharedConnection.execute(reqCtx, dropOutputViewQuery);
            HiveSchemaExecutor.LOGGER.debug("Trying to execute query: {}", (Object)createOutputViewQuery);
            SharedConnection.execute(reqCtx, createOutputViewQuery);
            SharedConnection.execute(reqCtx, reqCtx.getPoolingCriteria(), connection -> {
                try {
                    HiveSchemaExecutor.LOGGER.debug("Trying to fetch schema details");
                    final BaseSchemaHandler schemaInstance = SchemaFactory.createSchemaInstance(reqCtx);
                    final List<TableColumn> tableOutputSchema = (List<TableColumn>)schemaInstance.getTableSchema(reqCtx, AwbConfigs.getAWBWorkspaceSchema(), task.getSelectTargetNode().getId(), true);
                    final ExecutionResult result = new ExecutionResult(task.getSelectTargetNode());
                    HiveMetaDataUtils.populateSchema(tableOutputSchema, result);
                    ctx.set(task.getOutputKey(), (TaskOutput)result);
                }
                catch (Exception e) {
                    Throwables.propagate((Throwable)e);
                }
            });
            SharedConnection.execute(reqCtx, dropOutputViewQuery);
        }
        catch (Exception e) {
            HiveSchemaExecutor.LOGGER.error("Error while trying to execute task {}", (Object)task.getId());
            HiveSchemaExecutor.LOGGER.error("Exception: {}", (Object)AwbUtil.getErrorMessage((Throwable)e));
            IWRunTimeException.propagate(e, "SCHEMA_EXECUTION_ERROR");
        }
    }
    
    static {
        LOGGER = LoggerFactory.getLogger((Class)HiveSchemaExecutor.class);
    }
}
