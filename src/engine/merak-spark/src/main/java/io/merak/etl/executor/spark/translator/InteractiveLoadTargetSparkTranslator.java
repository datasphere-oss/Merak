package io.merak.etl.executor.spark.translator;

import io.merak.etl.pipeline.dto.*;
import io.merak.etl.translator.*;
import io.merak.etl.executor.spark.task.*;
import io.merak.etl.executor.spark.translator.dto.*;
import io.merak.etl.sdk.task.*;
import io.merak.etl.sdk.pipeline.*;
import io.merak.etl.sdk.translator.*;

import org.apache.spark.sql.*;

public class InteractiveLoadTargetSparkTranslator extends LoadTargetSparkTranslator
{
    protected PipelineNode parentNode;
    
    public InteractiveLoadTargetSparkTranslator(final LoadTarget node, final TranslatorContext translatorContext) {
        super(node, translatorContext);
        this.parentNode = this.getParentNode((PipelineNode)node);
    }
    
    @Override
    public TaskNode translate() {
        this.LOGGER.trace("entering translator for InteractiveTargetTranslator");
        if (this.requestContext.getProcessingContext().isValidationMode()) {
            this.LOGGER.trace("creating a SparkValidatorTaskNode ");
            this.sparkTranslatorState.addTranslations(this::generateValidateDataFrame);
            return (TaskNode)new SparkValidatorTaskNode((SinkTranslator)this, (EndVertex)this.node, this.sparkTranslatorState);
        }
        if (this.requestContext.getProcessingContext().isSchemaMode()) {
            this.LOGGER.trace("Creating a SparkSchemaTaskNode.");
            this.sparkTranslatorState.addTranslations(this::generateSelectDataFrame);
            return (TaskNode)new SparkSchemaTaskNode((SinkTranslator)this, (EndVertex)this.node, this.sparkTranslatorState);
        }
        final String selectQuery = String.format("select * from `%s`.`%s`", ((LoadTarget)this.node).getSchemaName(), ((LoadTarget)this.node).getTableName());
        this.LOGGER.trace("creating SparkSelectTableTaskNode for table {}", (Object)selectQuery);
        this.sparkTranslatorState.addTranslations(this::generateSelectDataFrame);
        return (TaskNode)new SparkSelectTableTaskNode((SinkTranslator)this, (EndVertex)this.node, this.sparkTranslatorState);
    }
    
    private void generateValidateDataFrame() {
        final Dataset<Row> inputDF = this.getDataset(this.parentNode);
        this.LOGGER.trace("adding dataframe object for targetnode {} stateObject is {}", (Object)this.node, (Object)this.sparkTranslatorState.toString());
        final DataFrameObject dataFrameObject = new DataFrameObject(inputDF, "target validation step");
        this.sparkTranslatorState.addDataFrame(this.node, dataFrameObject);
    }
    
    private void generateCreateDataFrame() {
        final Dataset<Row> parentDataset = this.getDataset(this.parentNode);
        final Dataset<Row> targetDataSet = this.select(this.node, parentDataset);
        final DataFrameObject dataFrameObject = new DataFrameObject(targetDataSet, "target create step");
        this.sparkTranslatorState.addDataFrame(this.node, dataFrameObject);
    }
    
    private void generateSelectDataFrame() {
        this.LOGGER.trace("getting dataframe object for select from targetnode {} stateObject is {}", (Object)this.node, (Object)this.sparkTranslatorState.toString());
        final Dataset<Row> parentDataSet = this.getDataset(this.parentNode);
        final Dataset<Row> targetDataSet = this.select(parentDataSet);
        final DataFrameObject dataFrameObject = new DataFrameObject(targetDataSet, "target select step");
        this.sparkTranslatorState.addDataFrame(this.node, dataFrameObject);
    }
    
    @Override
    protected void generateDataFrame() {
    }
}
