package io.merak.etl.executor.hive.translator.previewsql;

import com.google.common.base.*;

import io.merak.etl.context.*;
import io.merak.etl.pipeline.dto.*;
import io.merak.etl.pipeline.dto.pivot.*;
import io.merak.etl.sdk.engine.*;
import io.merak.etl.translator.*;
import io.merak.etl.utils.constants.*;

import org.slf4j.*;

public class NodeSqlTranslatorStore
{
    private static final Logger LOGGER;
    private RequestContext requestContext;
    private TranslatorMap<Class, Class> nodeToTranslatorMap;
    
    public NodeSqlTranslatorStore(final RequestContext requestContext) {
        this.nodeToTranslatorMap = (TranslatorMap<Class, Class>)new TranslatorMap();
        this.requestContext = requestContext;
        this.populate(this.nodeToTranslatorMap);
    }
    
    public void populate(final TranslatorMap<Class, Class> translatorMap) {
        translatorMap.put(this.getBatchEngine(), (Object)SourceNode.class, (Object)SourceTranslator.class);
        translatorMap.put(this.getBatchEngine(), (Object)FilterNode.class, (Object)FilterTranslator.class);
        translatorMap.put(this.getBatchEngine(), (Object)DeriveNode.class, (Object)DeriveTranslator.class);
        translatorMap.put(this.getBatchEngine(), (Object)AggregateNode.class, (Object)AggregateTranslator.class);
        translatorMap.put(this.getBatchEngine(), (Object)DistinctNode.class, (Object)DistinctTranslator.class);
        translatorMap.put(this.getBatchEngine(), (Object)JoinNode.class, (Object)JoinTranslator.class);
        translatorMap.put(this.getBatchEngine(), (Object)UnionNode.class, (Object)UnionTranslator.class);
        translatorMap.put(this.getBatchEngine(), (Object)UnnestNode.class, (Object)UnnestTranslator.class);
        translatorMap.put(this.getBatchEngine(), (Object)InNotInNode.class, (Object)InNotInTranslator.class);
        translatorMap.put(this.getBatchEngine(), (Object)ExistsNode.class, (Object)ExistsTranslator.class);
        translatorMap.put(this.getBatchEngine(), (Object)CleanseNode.class, (Object)CleanseTranslator.class);
        translatorMap.put(this.getBatchEngine(), (Object)PivotNode.class, (Object)PivotTranslator.class);
        translatorMap.put(this.getBatchEngine(), (Object)LookupNode.class, (Object)LookupTranslator.class);
        translatorMap.put(this.getBatchEngine(), (Object)SnowFlakeTargetNode.class, (Object)SnowflakeTranslator.class);
        translatorMap.put(this.getBatchEngine(), (Object)AzureDWNode.class, (Object)AzureDWTranslator.class);
        translatorMap.put(this.getBatchEngine(), AwbConstants.BuildMode.INTERACTIVE, (Object)TargetNode.class, (Object)InteractiveTargetTranslator.class);
        translatorMap.put(this.getBatchEngine(), AwbConstants.BuildMode.INTERACTIVE, (Object)MapRDBKeyStoreTarget.class, (Object)MaprDBInteractiveTargetTranslator.class);
    }
    
    public NodeSqlTranslator createTranslator(final PipelineNode node, final TranslatorContext ctxt) {
        try {
            final AwbConstants.BuildMode buildMode = this.requestContext.getProcessingContext().getBuildMode();
            final BatchEngine engine = this.requestContext.getBatchEngine();
            final Class translatorClass = (Class)this.nodeToTranslatorMap.get(engine, buildMode, (Object)node.getClass());
            return (NodeSqlTranslator)translatorClass.getConstructors()[0].newInstance(node, ctxt);
        }
        catch (Exception e) {
            NodeSqlTranslatorStore.LOGGER.error("Failed to create translator for node {}.", (Object)node);
            NodeSqlTranslatorStore.LOGGER.debug("Exception: {}", (Object)Throwables.getStackTraceAsString((Throwable)e));
            Throwables.propagate((Throwable)e);
            throw new UnsupportedOperationException("Not reachable!");
        }
    }
    
    public BatchEngine getBatchEngine() {
        return this.requestContext.getBatchEngine();
    }
    
    static {
        LOGGER = LoggerFactory.getLogger((Class)NodeSqlTranslatorStore.class);
    }
}
