package io.merak.etl.executor.hive.executor.impl;

import org.slf4j.*;

import io.merak.etl.context.*;
import io.merak.etl.executor.hive.session.*;
import io.merak.etl.executor.hive.task.*;
import io.merak.etl.executor.impl.*;
import io.merak.etl.pipeline.dto.*;
import io.merak.etl.sdk.executor.*;
import io.merak.etl.sdk.task.*;
import io.merak.etl.utils.column.*;
import io.merak.etl.utils.exception.*;

import com.google.common.collect.*;

import java.util.*;

public class HiveSchemaSyncExecutor implements TaskExecutor
{
    protected Logger LOGGER;
    
    public HiveSchemaSyncExecutor() {
        this.LOGGER = LoggerFactory.getLogger((Class)this.getClass());
    }
    
    public void execute(final Task taskNode, final TaskExecutorContext ctx) {
        final RequestContext requestContext = (RequestContext)ctx.getContext();
        final HiveSchemaSyncTaskNode syncTaskNode = (HiveSchemaSyncTaskNode)taskNode;
        final TargetNode targetNode = syncTaskNode.getTargetNode();
        final SqlBatchQueryExecutor taskExecutor = (SqlBatchQueryExecutor)TaskExecutorFactory.getExecutor((Task)syncTaskNode.getTargetTaskNode(), requestContext);
        taskExecutor.init(requestContext, (BatchQueryTaskNode)syncTaskNode.getTargetTaskNode());
        final Map<String, String> schema = taskExecutor.getSchema();
        final List<Entity> deltaDifferenceMainEntites = (List<Entity>)targetNode.getDeltaDifferenceMainEntities();
        final List<TableColumn> columnList = new ArrayList<TableColumn>();
        for (final Entity entity : deltaDifferenceMainEntites) {
            if (schema.containsKey(entity.getGivenName())) {
                final TableColumn tableColumn = new TableColumn(entity.getGivenName(), (String)schema.get(entity.getGivenName()));
                columnList.add(tableColumn);
                this.LOGGER.debug("information of additive schema with datatype is {},{}", (Object)tableColumn.getName(), (Object)tableColumn.getType());
            }
        }
        final String schemaName = targetNode.getSchemaName();
        final String tableName = targetNode.getTableName();
        final List<String> stmts = this.addColumns(schemaName, tableName, columnList);
        this.LOGGER.info("schema sync stmt is {}", (Object)stmts.toString());
        final HiveJdbcSession hiveJdbcSession = new HiveJdbcSession(requestContext);
        try {
            hiveJdbcSession.executeDDL(stmts);
        }
        catch (Exception e) {
            this.LOGGER.error("error while executing addColumn statements: {}", (Object)stmts);
        }
        final ExecutionMetaDataUtils metaDataUtils = new ExecutionMetaDataUtils();
        final List<Entity> outputEntities = (List<Entity>)metaDataUtils.getResetOutputEntities(targetNode, requestContext);
        targetNode.setOrderedOutputEntities((List)outputEntities);
        targetNode.setDeltaDifferenceMainEntities((List)Lists.newArrayList());
    }
    
    protected String addColumn(final String schemaName, final String tableName, final String columnName, final String columnType) {
        return String.format("ALTER TABLE `%s`.`%s` ADD COLUMNS(%s %s)", schemaName, tableName, columnName, columnType);
    }
    
    protected List<String> addColumns(final String schemaName, final String tableName, final List<TableColumn> columnList) {
        final List<String> stmts = (List<String>)Lists.newArrayList();
        for (final TableColumn tableColumn : columnList) {
            stmts.add(this.addColumn(schemaName, tableName, tableColumn.getName(), tableColumn.getType()));
        }
        return stmts;
    }
}
