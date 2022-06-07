package io.merak.etl.executor.spark.executor;

import org.slf4j.*;
import io.merak.etl.sdk.executor.*;
import io.merak.etl.context.*;
import io.merak.etl.executor.impl.*;
import io.merak.etl.sdk.task.*;
import io.merak.etl.translator.*;
import io.merak.etl.utils.exception.*;
import io.merak.etl.executor.spark.task.*;
import io.merak.etl.executor.spark.utils.*;
import scala.collection.*;
import com.google.common.collect.*;
import io.merak.adapters.filesystem.*;
import io.merak.etl.pipeline.dto.*;
import java.util.*;
import org.apache.spark.sql.*;

public class SparkHiveCompatibleSchemaSyncExecutor implements TaskExecutor
{
    protected Logger LOGGER;
    
    public SparkHiveCompatibleSchemaSyncExecutor() {
        this.LOGGER = LoggerFactory.getLogger((Class)this.getClass());
    }
    
    public void execute(final Task taskNode, final TaskExecutorContext ctx) {
        final RequestContext requestContext = (RequestContext)ctx.getContext();
        final SparkHiveCompatibleSchemaSyncTaskNode syncTaskNode = (SparkHiveCompatibleSchemaSyncTaskNode)taskNode;
        final List<Entity> deltaDifferenceMainEntites = (List<Entity>)syncTaskNode.getTargetNode().getDeltaDifferenceMainEntities();
        final HiveCompatibleSparkExecutor batchExecutor = (HiveCompatibleSparkExecutor)TaskExecutorFactory.getExecutor((Task)syncTaskNode.getTargetTaskNode(), requestContext);
        final SparkBatchTaskNode taskForTargetNode = (SparkBatchTaskNode)syncTaskNode.getTargetTaskNode();
        batchExecutor.init((TaskNode)taskForTargetNode, ctx);
        final Map<String, String> schema = taskForTargetNode.getSinkTranslator().getColumnsFromSchema();
        final TargetNode targetNode = taskForTargetNode.getSink();
        final String futureLocation = targetNode.getFutureTableLoc();
        this.LOGGER.info("current table location is {}", (Object)targetNode.getCurrentTableLoc());
        Dataset<Row> currentTable = batchExecutor.getDataSet(targetNode.getCurrentTableLoc());
        try {
            batchExecutor.executeSparkStatement(TranslatorUtils.getCreateDatabaseStatement(requestContext, targetNode.getSchemaName()));
        }
        catch (Exception e) {
            this.LOGGER.warn("Database statement failed {}", (Object)e.getMessage());
        }
        final List<String> stmts = taskForTargetNode.getSinkTranslator().overWriteHiveCompatible(this.getWorkingAdditiveTempTable(targetNode), futureLocation);
        this.LOGGER.info("statement for temp table creation {}", (Object)stmts.toString());
        try {
            batchExecutor.executeSparkStatement(stmts);
        }
        catch (Exception e2) {
            this.LOGGER.warn("statement execution failed {}", (Object)e2.getMessage());
        }
        for (final Entity entity : deltaDifferenceMainEntites) {
            currentTable = (Dataset<Row>)currentTable.withColumn(entity.getGivenName(), functions.lit((Object)null).cast((String)schema.get(entity.getGivenName())));
        }
        final ExecutionMetaDataUtils metaDataUtils = new ExecutionMetaDataUtils();
        final List<String> selectList = (List<String>)metaDataUtils.getSparkHiveCompatibleSelectList(targetNode);
        final SparkSession sparkSession = taskForTargetNode.getSinkTranslator().getSpark();
        currentTable.select((Seq)SparkUtils.getColumnSeqFromExpressions(sparkSession, selectList)).write().mode(SaveMode.Overwrite).format(targetNode.getStorageFormat()).insertInto(this.getWorkingAdditiveTempTable(targetNode));
        taskForTargetNode.getSinkTranslator().switchTable(this.getWorkingAdditiveTempTable(targetNode), this.getCurrentTableName(targetNode));
        targetNode.setFutureTableLoc(targetNode.getCurrentTableLoc());
        targetNode.setCurrentTableLoc(futureLocation);
        targetNode.setCurrDataSubDirectory(targetNode.getFutureSubDirectory());
        targetNode.setFutureSubDirectory();
        final List<Entity> outputEntities = (List<Entity>)metaDataUtils.getResetOutputEntities(syncTaskNode.getTargetNode(), requestContext);
        syncTaskNode.getTargetNode().setOrderedOutputEntities((List)outputEntities);
        syncTaskNode.getTargetNode().setDeltaDifferenceMainEntities((List)Lists.newArrayList());
        try (final IWFileSystem fileSystem = IWFileSystem.getFileSystemFromPath(syncTaskNode.getTargetNode().getHdfsLocation())) {
            fileSystem.getHdfsUtils().deleteDirIfExists(targetNode.getFutureTableLoc());
        }
        catch (Exception e3) {
            this.LOGGER.warn("HDFS Delete failed {}", (Object)e3.getMessage());
        }
    }
    
    private String getWorkingAdditiveTempTable(final TargetNode targetNode) {
        return String.format("`%s`.`%s_%s`", targetNode.getSchemaName(), targetNode.getTableName(), "tmp");
    }
    
    private String getCurrentTableName(final TargetNode targetNode) {
        return String.format("`%s`.`%s`", targetNode.getSchemaName(), targetNode.getTableName());
    }
}
