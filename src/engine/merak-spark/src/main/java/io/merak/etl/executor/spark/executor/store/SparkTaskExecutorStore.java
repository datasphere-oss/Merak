package io.merak.etl.executor.spark.executor.store;

import io.merak.etl.sdk.task.*;
import io.merak.etl.sdk.executor.*;
import io.merak.etl.task.impl.*;
import io.merak.etl.executor.spark.executor.*;
import io.merak.etl.executor.spark.executor.analytics.*;
import io.merak.etl.executor.spark.executor.extensions.*;
import io.merak.etl.executor.spark.task.*;
import io.merak.etl.executor.spark.task.extensions.*;
import io.merak.etl.executor.impl.*;

import org.slf4j.*;

public class SparkTaskExecutorStore implements TaskExecutorStore
{
    private static Logger logger;
    
    public TaskExecutor getTaskExecutor(final Task task) {
        if (task instanceof RunnableTaskNode) {
            return (TaskExecutor)new RunnableTaskExecutor();
        }
        if (task instanceof SparkBatchTaskNode && ((SparkBatchTaskNode)task).getSink().isHiveCompatible()) {
            return (TaskExecutor)new HiveCompatibleSparkExecutor();
        }
        if (task instanceof SparkBatchTaskNode && !((SparkBatchTaskNode)task).getSink().isHiveCompatible()) {
            return (TaskExecutor)new NonHiveCompatibleSparkExecutor();
        }
        if (task instanceof SparkDropTableTaskNode) {
            return (TaskExecutor)new DropTableExecutor();
        }
        if (task instanceof SparkSelectTableTaskNode) {
            return (TaskExecutor)new SparkInteractiveSelectExecutor();
        }
        if (task instanceof SparkSchemaTaskNode) {
            return (TaskExecutor)new SparkSchemaExecutor();
        }
        if (task instanceof SparkValidatorTaskNode) {
            return (TaskExecutor)new SparkInteractiveValidateExecutor();
        }
        if (task instanceof SparkCreateTableTaskNode) {
            return (TaskExecutor)new SparkInteractiveCreateExecutor();
        }
        if (task instanceof SparkQueryTaskNode) {
            return (TaskExecutor)new SparkQueryExecutor();
        }
        if (task instanceof SparkHiveCompatibleSchemaSyncTaskNode) {
            return (TaskExecutor)new SparkHiveCompatibleSchemaSyncExecutor();
        }
        if (task instanceof SparkNonHiveSchemaSyncTaskNode) {
            return (TaskExecutor)new SparkNonHiveSchemaSyncExecutor();
        }
        if (task instanceof SparkCustomTargetBatchTaskNode) {
            return (TaskExecutor)new SparkCustomTargetBatchExecutor();
        }
        if (task instanceof SparkCustomTargetInteractiveTaskNode) {
            return (TaskExecutor)new SparkCustomTargetInteractiveExecutor();
        }
        if (task instanceof SparkSnowFlakeTaskNode) {
            return (TaskExecutor)new SnowFlakeTargetExecutor();
        }
        if (task instanceof SparkCopySampleTaskNode) {
            return (TaskExecutor)new SparkCopySampleExecutor();
        }
        if (task instanceof SparkAnalyticsTaskNode) {
            return (TaskExecutor)new AnalyticsExecutor();
        }
        if (task instanceof SparkAnalyticsModelExportTaskNode) {
            return (TaskExecutor)new AnalyticsModelExportExecutor();
        }
        if (task instanceof SparkAzureDWTaskNode) {
            return (TaskExecutor)new AzureDWTargetExecutor();
        }
        final String msg = String.format("No executor found for invalid task type : %s", task.getClass().getName());
        SparkTaskExecutorStore.logger.error(msg);
        throw new UnsupportedOperationException(msg);
    }
    
    static {
        SparkTaskExecutorStore.logger = LoggerFactory.getLogger((Class)SparkTaskExecutorStore.class);
    }
}
