package io.merak.etl.executor.spark.translator;

import io.merak.etl.translator.*;
import io.merak.etl.pipeline.dto.*;
import io.merak.etl.sdk.task.*;
import io.merak.etl.sdk.pipeline.*;
import io.merak.etl.sdk.translator.*;
import io.merak.etl.utils.constants.*;
import io.merak.etl.executor.spark.task.*;
import io.merak.etl.executor.spark.translator.dto.*;

import org.apache.spark.sql.*;

public class SparkAnalyticsExportTranslator extends SparkTranslator<AnalyticsModelExportNode> implements SinkTranslator
{
    StringBuilder builder;
    
    public SparkAnalyticsExportTranslator(final AnalyticsModelExportNode node, final TranslatorContext translatorContext) {
        super((PipelineNode)node, translatorContext);
        this.builder = new StringBuilder();
    }
    
    @Override
    public TaskNode translate() {
        super.translate();
        if (this.requestContext.getProcessingContext().isValidationMode()) {
            return (TaskNode)new SparkValidatorTaskNode((SinkTranslator)this, (EndVertex)this.node, this.sparkTranslatorState);
        }
        if (this.requestContext.getProcessingContext().isSchemaMode()) {
            return (TaskNode)new SparkSchemaTaskNode((SinkTranslator)this, (EndVertex)this.node, this.sparkTranslatorState);
        }
        return (TaskNode)new SparkAnalyticsModelExportTaskNode((AnalyticsModelExportNode)this.node, this.getParent((PipelineNode)this.node), (TranslatorState)this.sparkTranslatorState);
    }
    
    @Override
    protected void generateDataFrame() {
        final PipelineNode parent = this.getParentNode((PipelineNode)this.node);
        final DataFrameObject incomingDFO = this.sparkTranslatorState.getDataFrame(parent);
        Dataset<Row> dataSet = incomingDFO.getDataset();
        dataSet = this.select(this.node, dataSet);
        if (this.requestContext.getProcessingContext().getBuildMode() == AwbConstants.BuildMode.INTERACTIVE) {
            dataSet = (Dataset<Row>)dataSet.limit(this.getSampleViewLimit());
        }
        final DataFrameObject datasetDFO = new DataFrameObject(dataSet, String.format("Dataset Sql: %s", this.builder.toString()));
        this.sparkTranslatorState.addDataFrame(this.node, datasetDFO);
    }
}
