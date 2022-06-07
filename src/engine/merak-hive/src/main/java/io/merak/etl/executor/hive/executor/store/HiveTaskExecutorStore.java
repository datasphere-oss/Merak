package io.merak.etl.executor.hive.executor.store;

import io.merak.etl.executor.hive.executor.impl.*;
import io.merak.etl.executor.hive.task.*;
import io.merak.etl.executor.impl.*;
import io.merak.etl.sdk.executor.*;
import io.merak.etl.sdk.task.*;
import io.merak.etl.task.impl.*;

import org.slf4j.*;

public class HiveTaskExecutorStore implements TaskExecutorStore
{
    private static Logger logger;
    
    public TaskExecutor getTaskExecutor(final Task task) {
        if (task instanceof KeyStoreTargetTaskNode) {
            return (TaskExecutor)new MapRDBHiveExecutor();
        }
        if (task instanceof LoadTargetInteractiveSelectQueryTaskNode) {
            return (TaskExecutor)new LoadTargetHiveInteractiveExecutor();
        }
        if (task instanceof InteractiveCreateQueryTaskNode) {
            return (TaskExecutor)new InteractiveCreateQueryExecutor();
        }
        if (task instanceof InteractiveSelectQueryTaskNode) {
            return (TaskExecutor)new InteractiveSelectQueryExecutor();
        }
        if (task instanceof HiveSchemaTaskNode) {
            return (TaskExecutor)new HiveSchemaExecutor();
        }
        if (task instanceof BatchQueryTaskNode) {
            return (TaskExecutor)new SqlBatchQueryExecutor();
        }
        if (task instanceof QueryTaskNode) {
            return (TaskExecutor)new QueryExecutor();
        }
        if (task instanceof RunnableTaskNode) {
            return (TaskExecutor)new RunnableTaskExecutor();
        }
        if (task instanceof CopySampleTaskNode) {
            return (TaskExecutor)new SqlCopySampleExecutor();
        }
        if (task instanceof AnalyticsTaskNode) {
            return (TaskExecutor)new AnalyticsExecutor();
        }
        if (task instanceof AnalyticsModelExportTaskNode) {
            return (TaskExecutor)new AnalyticsModelExportExecutor();
        }
        if (task instanceof HiveSchemaSyncTaskNode) {
            return (TaskExecutor)new HiveSchemaSyncExecutor();
        }
        final String msg = String.format("No executor found for invalid task type : %s", task.getClass().getName());
        HiveTaskExecutorStore.logger.error(msg);
        throw new UnsupportedOperationException(msg);
    }
    
    static {
        HiveTaskExecutorStore.logger = LoggerFactory.getLogger((Class)TaskExecutorFactory.class);
    }
}
