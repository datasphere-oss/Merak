package io.merak.etl.executor.spark.translator;

import io.merak.etl.translator.*;
import io.merak.etl.sdk.task.*;
import io.merak.etl.sdk.translator.*;
import com.google.common.collect.*;
import io.merak.etl.pipeline.dto.*;
import io.merak.etl.utils.constants.*;
import io.merak.etl.executor.spark.task.*;
import io.merak.etl.executor.spark.translator.dto.*;
import io.merak.etl.executor.spark.utils.*;

import java.util.function.*;

import org.apache.spark.sql.*;
import java.util.*;
import io.merak.etl.sdk.predictor.*;
import io.merak.etl.sdk.execution.factory.*;
import io.merak.etl.sdk.context.*;
import io.merak.etl.sdk.pipeline.*;
import io.merak.etl.sdk.execution.api.*;
import io.merak.etl.utils.config.*;
import java.util.stream.*;
import org.slf4j.*;

public class SparkAnalyticsTranslator extends SparkTranslator<AnalyticsNode>
{
    private static final Logger LOGGER;
    StringBuilder builder;
    
    public SparkAnalyticsTranslator(final AnalyticsNode node, final TranslatorContext translatorContext) {
        super((PipelineNode)node, translatorContext);
        this.builder = new StringBuilder();
    }
    
    @Override
    public TaskNode translate() {
        super.translate();
        if (this.isEndNode((PipelineNode)this.node)) {
            SparkAnalyticsTranslator.LOGGER.debug("Creating task node for {}", (Object)((AnalyticsNode)this.node).getId());
            return (TaskNode)new SparkAnalyticsTaskNode((AnalyticsNode)this.node, this.getPredictor(), this.getSampleDataQuery(), (TranslatorState)this.sparkTranslatorState);
        }
        SparkAnalyticsTranslator.LOGGER.debug("Intermediate node : Returning task node as null");
        return null;
    }
    
    @Override
    protected void generateDataFrame() {
        final PipelineNode parent = this.getParentNode((PipelineNode)this.node);
        final DataFrameObject incomingDFO = this.sparkTranslatorState.getDataFrame(parent);
        Dataset<Row> parentSet = incomingDFO.getDataset();
        SparkAnalyticsTranslator.LOGGER.debug(String.format("builder in if before %s ", this.builder.toString()));
        final String parentNode = "parentSet";
        if (this.requestContext.getProcessingContext().isValidationMode() || this.requestContext.getProcessingContext().isSchemaMode()) {
            SparkAnalyticsTranslator.LOGGER.debug("Creating dataset for validation mode..");
            final Map<String, String> columnMap = (Map<String, String>)Maps.newHashMap();
            final List<String> derivedColumns = ((AnalyticsNode)this.node).getOutputEntitiesUsed(true).filter(i -> i.containsType(Column.COLUMN_TYPE.PREDICTION) && ((Entity)i).getReferenceType().equals((Object)Entity.Mapping.REFERENCE_TYPE.NODE_OUTPUT)).map(i -> columnMap.put(i.getInputName(), "CAST(null AS double)")).collect((Collector<? super Object, ?, List<String>>)Collectors.toList());
            SparkAnalyticsTranslator.LOGGER.debug("Derive columnMap for validation {} ", (Object)columnMap.toString());
            for (final Map.Entry<String, String> column : columnMap.entrySet()) {
                parentSet = (Dataset<Row>)parentSet.withColumn((String)column.getKey(), SparkUtils.getColumnFromExp(this.spark, column.getValue()));
            }
            if (this.requestContext.getProcessingContext().getBuildMode() == AwbConstants.BuildMode.INTERACTIVE) {
                parentSet = (Dataset<Row>)parentSet.limit(this.getSampleViewLimit());
            }
        }
        else {
            SparkAnalyticsTranslator.LOGGER.debug("Creating dataframe for execution mode...");
            final Map<String, String> columnMap = (Map<String, String>)((AnalyticsNode)this.node).getInputEntities().stream().filter(i -> i.getReferencedEntity() != null).collect(Collectors.toMap((Function<? super Object, ?>)Entity::getInputName, c -> c.getOutputName((PipelineNode)this.node)));
            SparkAnalyticsTranslator.LOGGER.debug("Derive Column Map for execution {} ", (Object)columnMap.toString());
            SparkAnalyticsTranslator.LOGGER.debug("Schema before select:{}", (Object)parentSet.schema().treeString());
            parentSet = this.select((PipelineNode)this.node, c -> String.format("%s AS %s", c.getInputName(), c.getGivenName()), parentSet, i -> ((AnalyticsNode)this.node).getInputEntitiesUsed(true));
            SparkAnalyticsTranslator.LOGGER.debug("Schema after select: {}", (Object)parentSet.schema().treeString());
        }
        parentSet.createOrReplaceTempView(parentNode);
        final DataFrameObject datasetDFO = new DataFrameObject(parentSet, String.format("Dataset Sql: %s", this.builder.toString()));
        this.sparkTranslatorState.addDataFrame(this.node, datasetDFO);
    }
    
    private Predictor getPredictor() {
        final String parentNode = "parentSet";
        this.builder.append(String.format("select * from %s ", parentNode));
        final String inputSql = this.builder.toString();
        SparkAnalyticsTranslator.LOGGER.debug("ML Lib: {}, Input SQL for predictor: {}", (Object)this.requestContext.getMachineLearningEngine(), (Object)inputSql);
        final PredictorHandler predictorHandler = ExecutionHandler.getInstance().getPredictorHandler(this.requestContext.getMachineLearningEngine());
        return predictorHandler.getPredictor((Context)this.requestContext, inputSql, (Vertex)this.node);
    }
    
    private String getSampleDataQuery() {
        return String.format("SELECT %s FROM %s.%s LIMIT %d", ((AnalyticsNode)this.node).getOutputEntitiesUsed(false).map(c -> c.getOutputName((PipelineNode)this.node)).collect((Collector<? super Object, ?, String>)Collectors.joining(", ")), AwbUtil.escapeSqlIdentifier(((AnalyticsNode)this.node).getSchemaName()), AwbUtil.escapeSqlIdentifier(((AnalyticsNode)this.node).getTableName()), this.getSampleViewLimit());
    }
    
    @Override
    protected boolean canUseStarInSelect(final Function<Boolean, Stream<Entity>> entitiesUsedFormatter) {
        return false;
    }
    
    static {
        LOGGER = LoggerFactory.getLogger((Class)SparkAnalyticsTranslator.class);
    }
}
