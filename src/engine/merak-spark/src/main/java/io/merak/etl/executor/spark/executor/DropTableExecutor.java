package io.merak.etl.executor.spark.executor;

import io.merak.etl.sdk.task.*;
import io.merak.etl.executor.spark.task.*;
import io.merak.etl.sdk.executor.*;

import org.apache.spark.sql.*;
import org.slf4j.*;

public class DropTableExecutor implements TaskExecutor
{
    private static final Logger LOGGER;
    
    public void execute(final Task task, final TaskExecutorContext ctx) {
        final SparkDropTableTaskNode cleanupTask = (SparkDropTableTaskNode)task;
        DropTableExecutor.LOGGER.debug("Cleaning up the cached dataset");
        final String cutPoint = String.format("%s.%s", cleanupTask.getNode().getSchemaName(), cleanupTask.getNode().getTableName());
        try {
            final Dataset<Row> cachedDataset = DataFrameCache.getDataSet(cutPoint);
            cachedDataset.unpersist();
            DropTableExecutor.LOGGER.debug("Successfully removed {} from spark cache", (Object)cutPoint);
        }
        catch (Exception e) {
            DropTableExecutor.LOGGER.error("Exception while removing cached datasets : {}", (Object)cutPoint);
        }
    }
    
    static {
        LOGGER = LoggerFactory.getLogger((Class)DropTableExecutor.class);
    }
}
