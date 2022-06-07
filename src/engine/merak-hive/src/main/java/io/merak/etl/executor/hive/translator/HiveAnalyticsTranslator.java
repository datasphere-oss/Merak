package io.merak.etl.executor.hive.translator;

import io.merak.etl.executor.hive.task.*;
import io.merak.etl.pipeline.dto.*;
import io.merak.etl.sdk.context.*;
import io.merak.etl.sdk.dag.api.*;
import io.merak.etl.sdk.execution.api.*;
import io.merak.etl.sdk.execution.factory.*;
import io.merak.etl.sdk.pipeline.*;
import io.merak.etl.sdk.predictor.*;
import io.merak.etl.sdk.task.*;
import io.merak.etl.sdk.translator.*;
import io.merak.etl.translator.*;
import io.merak.etl.utils.config.*;

import java.util.stream.*;
import org.slf4j.*;

public class HiveAnalyticsTranslator extends SinkTranslator
{
    private static final Logger LOGGER;
    protected final AnalyticsNode node;
    
    public HiveAnalyticsTranslator(final AnalyticsNode node, final TranslatorContext translatorContext) {
        super(translatorContext);
        this.node = node;
    }
    
    public TaskNode translate() {
        if (this.requestContext.getProcessingContext().isValidationMode() || this.requestContext.getProcessingContext().isSchemaMode()) {
            final String derivedColumns = this.node.getOutputEntitiesUsed(true).filter(i -> i.containsType(Column.COLUMN_TYPE.PREDICTION) && ((Entity)i).getReferenceType().equals((Object)Entity.Mapping.REFERENCE_TYPE.NODE_OUTPUT)).map(i -> String.format("CAST(null AS double) AS %s", i.getInputName())).collect((Collector<? super Object, ?, String>)Collectors.joining(", "));
            this.start(this.node.getId());
            this.builder.append(String.format("SELECT %s.*, %s", this.getParent((PipelineNode)this.node), derivedColumns));
            this.from(this.getParent((PipelineNode)this.node)).end();
            TaskNode taskNode = null;
            if (this.isEndNode((PipelineNode)this.node)) {
                taskNode = this.endTranslation((PipelineNode)this.node);
                if (taskNode == null) {
                    taskNode = (TaskNode)new QueryTaskNode(this.builder.toString());
                }
            }
            return taskNode;
        }
        return (TaskNode)new AnalyticsTaskNode(this.node, this.getPredictor(), this.getSampleDataQuery(), null);
    }
    
    private Predictor getPredictor() {
        final PipelineNode parent = this.translatorContext.getPipelineDag().getIncomingNodes((DagNode)this.node).get(0);
        this.with().select((PipelineNode)this.node, c -> String.format("%s AS %s", c.getInputName(), c.getGivenName()), i -> this.node.getInputEntitiesUsed(true)).from(parent.getId());
        final String inputSql = this.builder.toString();
        HiveAnalyticsTranslator.LOGGER.debug("Input SQL for predictor: {}", (Object)inputSql);
        final PredictorHandler predictorHandler = ExecutionHandler.getInstance().getPredictorHandler(this.requestContext.getMachineLearningEngine());
        return predictorHandler.getPredictor((Context)this.requestContext, inputSql, (Vertex)this.node);
    }
    
    private String getSampleDataQuery() {
        return String.format("SELECT %s FROM %s.%s LIMIT %d", this.node.getOutputEntitiesUsed(false).map(c -> c.getOutputName((PipelineNode)this.node)).collect((Collector<? super Object, ?, String>)Collectors.joining(", ")), AwbUtil.escapeSqlIdentifier(this.node.getSchemaName()), AwbUtil.escapeSqlIdentifier(this.node.getTableName()), this.getSampleViewLimit());
    }
    
    static {
        LOGGER = LoggerFactory.getLogger((Class)HiveAnalyticsTranslator.class);
    }
}
