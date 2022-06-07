package io.merak.etl.executor.spark.executor;

import org.slf4j.*;
import io.merak.etl.sdk.executor.*;
import io.merak.etl.context.*;
import io.merak.etl.executor.impl.*;
import io.merak.etl.sdk.task.*;
import io.merak.etl.utils.exception.*;
import io.merak.etl.executor.spark.task.*;
import io.merak.etl.executor.spark.utils.*;
import scala.collection.*;
import io.merak.adapters.filesystem.*;
import io.merak.etl.pipeline.dto.*;
import java.util.*;
import org.apache.spark.sql.*;

public class SparkNonHiveSchemaSyncExecutor implements TaskExecutor
{
    protected Logger LOGGER;
    
    public SparkNonHiveSchemaSyncExecutor() {
        this.LOGGER = LoggerFactory.getLogger((Class)this.getClass());
    }
    
    public void execute(final Task taskNode, final TaskExecutorContext ctx) {
        final RequestContext requestContext = (RequestContext)ctx.getContext();
        final SparkNonHiveSchemaSyncTaskNode syncTaskNode = (SparkNonHiveSchemaSyncTaskNode)taskNode;
        final TargetNode targetNode = syncTaskNode.getTargetNode();
        final SparkBatchTaskNode taskForTargetNode = (SparkBatchTaskNode)syncTaskNode.getTargetTaskNode();
        final NonHiveCompatibleSparkExecutor batchExecutor = (NonHiveCompatibleSparkExecutor)TaskExecutorFactory.getExecutor((Task)syncTaskNode.getTargetTaskNode(), requestContext);
        batchExecutor.init((TaskNode)taskForTargetNode, ctx);
        Dataset<Row> currentTable = batchExecutor.getDataSet(targetNode.getCurrentTableLoc());
        final Map<String, String> schema = taskForTargetNode.getSinkTranslator().getColumnsFromSchema();
        final List<String> stmts = taskForTargetNode.getSinkTranslator().generateDatabaseStmts(this.getWorkingAdditiveTempTable(targetNode));
        try {
            batchExecutor.executeSparkStatement(stmts);
        }
        catch (Exception e) {
            this.LOGGER.warn("Database statement failed {}", (Object)e.getMessage());
        }
        final String futureLocation = targetNode.getFutureTableLoc();
        final List<Entity> deltaDifferenceMainEntites = (List<Entity>)syncTaskNode.getTargetNode().getDeltaDifferenceMainEntities();
        for (final Entity entity : deltaDifferenceMainEntites) {
            if (schema.containsKey(entity.getGivenName())) {
                currentTable = (Dataset<Row>)currentTable.withColumn(entity.getGivenName(), functions.lit((Object)null).cast((String)schema.get(entity.getGivenName())));
            }
        }
        this.LOGGER.info("after select from translator schema is {}", (Object)currentTable.schema().toString());
        final ExecutionMetaDataUtils metaDataUtils = new ExecutionMetaDataUtils();
        final SparkSession sparkSession = taskForTargetNode.getSinkTranslator().getSpark();
        final List<String> selectList = (List<String>)metaDataUtils.getSparkNonHiveCompatibleSelectList(targetNode);
        currentTable = (Dataset<Row>)currentTable.select((Seq)SparkUtils.getColumnSeqFromExpressions(sparkSession, selectList));
        DataFrameWriter dataFrameWriter = taskForTargetNode.getSinkTranslator().getNonHiveCompatibleWriter(targetNode, currentTable);
        dataFrameWriter = dataFrameWriter.format(targetNode.getStorageFormat()).mode(SaveMode.Overwrite).option("path", futureLocation);
        dataFrameWriter.saveAsTable(this.getWorkingAdditiveTempTable(targetNode));
        taskForTargetNode.getSinkTranslator().switchTable(this.getWorkingAdditiveTempTable(targetNode), this.getCurrentTableName(targetNode));
        taskForTargetNode.getSink().setFutureTableLoc(targetNode.getCurrentTableLoc());
        taskForTargetNode.getSink().setCurrentTableLoc(futureLocation);
        this.LOGGER.info("after schema sync current table location is {}", (Object)futureLocation);
        targetNode.setCurrDataSubDirectory(targetNode.getFutureSubDirectory());
        targetNode.setFutureSubDirectory();
        try (final IWFileSystem fileSystem = IWFileSystem.getFileSystemFromPath(targetNode.getHdfsLocation())) {
            fileSystem.getHdfsUtils().deleteDirIfExists(taskForTargetNode.getSink().getFutureTableLoc());
        }
        catch (Exception ex) {}
    }
    
    private String getWorkingAdditiveTempTable(final TargetNode targetNode) {
        return String.format("`%s`.`%s_%s`", targetNode.getSchemaName(), targetNode.getTableName(), "tmp");
    }
    
    private String getCurrentTableName(final TargetNode targetNode) {
        return String.format("`%s`.`%s`", targetNode.getSchemaName(), targetNode.getTableName());
    }
}
