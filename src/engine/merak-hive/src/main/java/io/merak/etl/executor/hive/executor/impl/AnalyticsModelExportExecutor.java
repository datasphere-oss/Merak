package io.merak.etl.executor.hive.executor.impl;

import io.merak.etl.context.*;
import io.merak.etl.executor.hive.task.*;
import io.merak.etl.metadata.*;
import io.merak.etl.pipeline.dto.*;
import io.merak.etl.sdk.executor.*;
import io.merak.etl.sdk.predictor.*;
import io.merak.etl.sdk.task.*;
import io.merak.etl.utils.config.*;
import io.merak.etl.utils.constants.*;

import com.google.common.base.*;
import merak.tools.ExceptionHandling.*;
import merak.tools.utils.*;
import org.slf4j.*;

public class AnalyticsModelExportExecutor implements TaskExecutor
{
    private static final Logger LOGGER;
    private static final String RANDOM_FOREST_PREDICTOR_KEY = "ANALYTICS_RANDOM_FOREST_CLASSIFICATION";
    
    public void execute(final Task task, final TaskExecutorContext ctx) {
        final RequestContext reqCtx = (RequestContext)ctx.getContext();
        final AnalyticsModelExportTaskNode exportTaskNode = (AnalyticsModelExportTaskNode)task;
        final AnalyticsModelExportNode exportNode = exportTaskNode.getExportNode();
        try {
            AnalyticsModelExportExecutor.LOGGER.debug("Executing Export Model Executor");
            if (reqCtx.getProcessingContext().getBuildMode() == AwbConstants.BuildMode.BATCH) {
                AnalyticsModelExportExecutor.LOGGER.info("Predictor Key = {}", (Object)exportTaskNode.getPredictorKey());
                AnalyticsModelExportExecutor.LOGGER.info("Predictor  = {}", (Object)ctx.get(exportTaskNode.getPredictorKey()));
                final Predictor predictor = (Predictor)ctx.get(exportTaskNode.getPredictorKey());
                Preconditions.checkNotNull((Object)predictor, (Object)"Predictor is null");
                final String modelSavePath = this.getExportPath(exportNode, false);
                String pmmlSavePath = null;
                if (!exportTaskNode.getPredictorKey().contains("ANALYTICS_RANDOM_FOREST_CLASSIFICATION")) {
                    pmmlSavePath = this.getExportPath(exportNode, true);
                }
                Preconditions.checkState(!Strings.isNullOrEmpty(modelSavePath), (Object)"Model Save Path Cannot be null or empty");
                predictor.saveModel(modelSavePath, pmmlSavePath);
                AwbMetastoreUtil.updateMetastore(reqCtx.getPipelineId(), exportNode);
            }
        }
        catch (Exception e) {
            AnalyticsModelExportExecutor.LOGGER.error("Error while trying to executing Analytics Model Export : {}", (Object)Throwables.getStackTraceAsString((Throwable)e));
            AnalyticsModelExportExecutor.LOGGER.error("Exception: {}", (Object)AwbUtil.getErrorMessage((Throwable)e));
            IWRunTimeException.propagate(e, "ANALYTICS_MODEL_EXPORT_EXECUTION_ERROR");
        }
    }
    
    private String getExportPath(final AnalyticsModelExportNode exportNode, final boolean isPmml) {
        try {
            if (!isPmml) {
                return AwbPathUtil.getAnalyticsModelPath(exportNode.getAnalyticsModelPath());
            }
            return String.format("%s%s", IWPathUtil.addSeparatorIfNotExists(AwbConfigs.getFileSystemPrefix()), exportNode.getAnalyticsModelPath() + "/pmml.xml");
        }
        catch (Exception e) {
            AnalyticsModelExportExecutor.LOGGER.error("Error creating directory : {}", (Object)Throwables.getStackTraceAsString((Throwable)e));
            Throwables.propagate((Throwable)e);
            return null;
        }
    }
    
    static {
        LOGGER = LoggerFactory.getLogger((Class)AnalyticsModelExportExecutor.class);
    }
}
