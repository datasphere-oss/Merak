package io.merak.etl.executor.hive.executor.impl;

import com.google.common.base.*;

import io.merak.etl.executor.hive.task.*;
import io.merak.etl.pipeline.dto.*;
import io.merak.etl.sdk.executor.*;
import io.merak.etl.sdk.predictor.*;
import io.merak.etl.sdk.task.*;
import io.merak.etl.utils.config.*;
import io.merak.tools.ExceptionHandling.*;

import org.slf4j.*;

public class AnalyticsExecutor implements TaskExecutor
{
    private static final Logger LOGGER;
    
    public void execute(final Task task, final TaskExecutorContext ctx) {
        final AnalyticsTaskNode taskNode = (AnalyticsTaskNode)task;
        final AnalyticsNode analyticsNode = taskNode.getAnalyticsNode();
        AnalyticsExecutor.LOGGER.debug("Start AnalyticsExecutor");
        try {
            final Predictor predictor = taskNode.getPredictor();
            predictor.predict();
            predictor.finish();
            Preconditions.checkState(ctx.get(analyticsNode.getId()) == null, (Object)"Analytics Predictor already exists");
            AnalyticsExecutor.LOGGER.info("Setting Predictor with Key = {}", (Object)analyticsNode.getId());
            ctx.set(analyticsNode.getId(), (TaskOutput)predictor);
        }
        catch (Exception e) {
            AnalyticsExecutor.LOGGER.error("Error while trying to execute batch query : {}", (Object)Throwables.getStackTraceAsString((Throwable)e));
            AnalyticsExecutor.LOGGER.error("Exception: {}", (Object)AwbUtil.getErrorMessage((Throwable)e));
            IWRunTimeException.propagate(e, "ANALYTICS_NODE_EXECUTION_ERROR");
        }
    }
    
    static {
        LOGGER = LoggerFactory.getLogger((Class)AnalyticsExecutor.class);
    }
}
