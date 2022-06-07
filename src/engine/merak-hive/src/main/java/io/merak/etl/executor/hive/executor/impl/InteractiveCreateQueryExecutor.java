package io.merak.etl.executor.hive.executor.impl;

import io.merak.adapters.filesystem.*;
import io.merak.etl.context.*;
import io.merak.etl.executor.hive.task.*;
import io.merak.etl.sdk.executor.*;
import io.merak.etl.sdk.task.*;
import io.merak.etl.utils.config.*;
import io.merak.etl.utils.shared.*;
import merak.tools.ExceptionHandling.*;

import org.slf4j.*;

public class InteractiveCreateQueryExecutor extends QueryExecutor
{
    private static final Logger LOGGER;
    
    @Override
    public void execute(final Task taskNode, final TaskExecutorContext ctx) {
        final RequestContext reqCtx = (RequestContext)ctx.getContext();
        final InteractiveCreateQueryTaskNode task = (InteractiveCreateQueryTaskNode)taskNode;
        final String query = task.getQuery();
        try {
            final IWFileSystem fileSystem = IWFileSystem.getFileSystemFromPath(task.getCreateTargetNode().getHdfsLocation());
            final HdfsUtils hdfsUtils = fileSystem.getHdfsUtils();
            hdfsUtils.createDirIfNotExists(task.getCreateTargetNode().getHdfsLocation(), true);
            SharedConnection.execute(reqCtx, query);
        }
        catch (Exception e) {
            InteractiveCreateQueryExecutor.LOGGER.error("Exception: {}", (Object)e.getMessage());
            InteractiveCreateQueryExecutor.LOGGER.error("Error while trying to execute task {}", (Object)task.getId());
            InteractiveCreateQueryExecutor.LOGGER.error("Exception: {}", (Object)AwbUtil.getErrorMessage((Throwable)e));
            IWRunTimeException.propagate(e, "INTERACTIVE_CREATE_QUERY_EXECUTION_ERROR");
        }
    }
    
    static {
        LOGGER = LoggerFactory.getLogger((Class)InteractiveCreateQueryExecutor.class);
    }
}
