package io.merak.etl.executor.spark.executor.extensions;

import org.slf4j.*;
import io.merak.etl.sdk.task.*;
import io.merak.etl.executor.spark.executor.*;
import io.merak.etl.context.*;
import io.merak.etl.pipeline.dto.*;
import io.merak.etl.sdk.executor.*;
import io.merak.etl.executor.impl.*;

public class SparkCustomTargetInteractiveExecutor extends SparkCustomTargetExecutor
{
    protected final Logger LOGGER;
    
    public SparkCustomTargetInteractiveExecutor() {
        this.LOGGER = LoggerFactory.getLogger((Class)this.getClass());
    }
    
    public void execute(final Task task, final TaskExecutorContext ctx) {
        final RequestContext reqCtx = (RequestContext)ctx.getContext();
        this.init(task);
        final ExecutionResult result = SparkHelper.generateResult(this.sparkTranslatorState, (PipelineNode)this.customTargetNode, reqCtx.getLimit());
        ctx.set(task.getOutputKey(), (TaskOutput)result);
        this.LOGGER.info(" added result count {} to key {}", (Object)result.getCount(), (Object)task.getOutputKey());
    }
}
