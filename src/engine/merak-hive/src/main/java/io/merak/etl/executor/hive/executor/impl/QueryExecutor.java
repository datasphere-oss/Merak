package io.merak.etl.executor.hive.executor.impl;

import io.merak.etl.context.*;
import io.merak.etl.executor.hive.task.*;
import io.merak.etl.sdk.executor.*;
import io.merak.etl.sdk.task.*;
import io.merak.etl.utils.config.*;
import io.merak.etl.utils.shared.*;
import merak.tools.ExceptionHandling.*;
import org.slf4j.*;

public class QueryExecutor implements TaskExecutor
{
    private static final Logger LOGGER;
    
    public void execute(final Task taskNode, final TaskExecutorContext ctx) {
        final RequestContext reqCtx = (RequestContext)ctx.getContext();
        final QueryTaskNode task = (QueryTaskNode)taskNode;
        final String query = task.getQuery();
        try {
            SharedConnection.execute(reqCtx, query);
        }
        catch (Exception e) {
            QueryExecutor.LOGGER.error("Error while trying to execute task {}", (Object)task.getId());
            QueryExecutor.LOGGER.error("Exception: {}", (Object)AwbUtil.getErrorMessage((Throwable)e));
            IWRunTimeException.propagate(e, "HIVE_EXECUTION_ERROR");
        }
    }
    
    static {
        LOGGER = LoggerFactory.getLogger((Class)QueryExecutor.class);
    }
}
