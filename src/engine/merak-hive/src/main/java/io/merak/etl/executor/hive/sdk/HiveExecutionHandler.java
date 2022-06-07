package io.merak.etl.executor.hive.sdk;

import io.merak.etl.context.*;
import io.merak.etl.executor.hive.executor.store.*;
import io.merak.etl.executor.hive.schema.*;
import io.merak.etl.executor.hive.session.*;
import io.merak.etl.executor.hive.task.*;
import io.merak.etl.executor.hive.translator.*;
import io.merak.etl.executor.hive.translator.previewsql.*;
import io.merak.etl.executor.hive.translator.store.*;
import io.merak.etl.pipeline.dto.*;
import io.merak.etl.sdk.context.*;
import io.merak.etl.sdk.dag.api.*;
import io.merak.etl.sdk.engine.*;
import io.merak.etl.sdk.execution.api.*;
import io.merak.etl.sdk.executor.*;
import io.merak.etl.sdk.pipeline.*;
import io.merak.etl.sdk.schema.*;
import io.merak.etl.sdk.session.*;
import io.merak.etl.sdk.task.*;
import io.merak.etl.sdk.translator.*;
import io.merak.etl.task.impl.*;
import io.merak.etl.translator.*;

import com.google.common.collect.*;
import com.google.common.base.*;

import java.util.*;
import org.slf4j.*;

@ExecutionAPI(engine = BatchEngine.HIVE)
public class HiveExecutionHandler implements BaseHandler
{
    private static final Logger LOGGER;
    
    public TaskNode getSampleTask(final String pipelineId, final SourceVertex source) {
        return (TaskNode)new CopySampleTaskNode(pipelineId, (SourceNode)source);
    }
    
    public TaskNode getSchemaSyncTask(final SinkVertex targetNode, final TaskNode targetTaskNode) {
        return (TaskNode)new HiveSchemaSyncTaskNode((TargetNode)targetNode, targetTaskNode);
    }
    
    public TranslatorState getTranslatorState(final Context context) {
        return (TranslatorState)new SqlTranslatorState();
    }
    
    public TranslatorStore getTranslatorStore() {
        return (TranslatorStore)new HiveTranslatorStore();
    }
    
    public ExecutionEngineSession getSession(final Context context) {
        return (ExecutionEngineSession)new HiveJdbcSession((RequestContext)context);
    }
    
    public SchemaHandler getSchemaHandler() {
        return (SchemaHandler)new HiveSchema();
    }
    
    public TaskExecutorStore getExecutorStore(final Context context) {
        return (TaskExecutorStore)new HiveTaskExecutorStore();
    }
    
    public String getPreviewQuery(final Context context) {
        final RequestContext requestContext = (RequestContext)context;
        final Dag<PipelineNode> ancestorsDag = (Dag<PipelineNode>)requestContext.getProcessingContext().getDag();
        final TranslatorContext translatorContext = new TranslatorContext(requestContext, (Dag)ancestorsDag, this.getTranslatorState((Context)requestContext));
        final NodeSqlTranslatorStore nodeSqlTranslator = new NodeSqlTranslatorStore(requestContext);
        final List<String> previewQueryList = (List<String>)Lists.newArrayList();
        final NodeSqlTranslatorStore nodeSqlTranslatorStore;
        final TranslatorContext ctxt;
        final TaskNode translate;
        final String query;
        final List<String> list;
        ancestorsDag.visitInTopologicalOrder(node -> {
            translate = nodeSqlTranslatorStore.createTranslator(node, ctxt).translate();
            Preconditions.checkNotNull((Object)translate, (Object)String.format("Sql Translator missing: %s", node.getName()));
            query = ((QueryTaskNode)translate).getQuery();
            list.add(0, query);
            HiveExecutionHandler.LOGGER.debug("Preview Query for Node {} - {}", (Object)node.getName(), (Object)query);
            return Boolean.valueOf(true);
        });
        return previewQueryList.get(0);
    }
    
    static {
        LOGGER = LoggerFactory.getLogger((Class)HiveExecutionHandler.class);
    }
}
