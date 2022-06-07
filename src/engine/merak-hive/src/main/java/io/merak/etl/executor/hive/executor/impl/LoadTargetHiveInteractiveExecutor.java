package io.merak.etl.executor.hive.executor.impl;

import io.merak.etl.context.*;
import io.merak.etl.executor.hive.metadata.*;
import io.merak.etl.executor.hive.task.*;
import io.merak.etl.executor.impl.*;
import io.merak.etl.sdk.executor.*;
import io.merak.etl.sdk.pipeline.*;
import io.merak.etl.sdk.task.*;
import io.merak.etl.utils.config.*;
import io.merak.etl.utils.shared.*;
import merak.tools.ExceptionHandling.*;

import java.sql.*;
import org.slf4j.*;

public class LoadTargetHiveInteractiveExecutor extends LoadTargetInteractiveExecutor
{
    private static final Logger LOGGER;
    
    public void execute(final Task taskNode, final TaskExecutorContext ctx) {
        final RequestContext reqCtx = (RequestContext)ctx.getContext();
        final LoadTargetInteractiveSelectQueryTaskNode task = (LoadTargetInteractiveSelectQueryTaskNode)taskNode;
        final String query = task.getQuery();
        try {
            SharedConnection.execute(reqCtx, reqCtx.getPoolingCriteria(), connection -> {
                LoadTargetHiveInteractiveExecutor.LOGGER.debug("Trying to execute query: {}", (Object)query);
                try (final Statement getSampleDataStatement = connection.createStatement();
                     final ResultSet sampleData = getSampleDataStatement.executeQuery(query)) {
                    final ExecutionResult result = new ExecutionResult((EndVertex)task.getSelectTargetNode());
                    HiveMetaDataUtils.populate(sampleData, result, reqCtx.isGetData());
                    ctx.set(task.getOutputKey(), (TaskOutput)result);
                }
            });
        }
        catch (Exception e) {
            LoadTargetHiveInteractiveExecutor.LOGGER.error("Error while trying to execute task {}", (Object)task.getId());
            LoadTargetHiveInteractiveExecutor.LOGGER.error("Exception: {}", (Object)AwbUtil.getErrorMessage((Throwable)e));
            IWRunTimeException.propagate(e, "INTERACTIVE_SELECT_QUERY_EXECUTION_ERROR");
        }
    }
    
    static {
        LOGGER = LoggerFactory.getLogger((Class)LoadTargetHiveInteractiveExecutor.class);
    }
}
