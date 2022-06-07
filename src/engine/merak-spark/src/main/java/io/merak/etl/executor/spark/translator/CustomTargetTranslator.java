package io.merak.etl.executor.spark.translator;

import io.merak.etl.sdk.translator.*;
import io.merak.etl.extensions.api.spark.*;
import io.merak.etl.translator.*;
import io.merak.etl.dispatcher.job.*;
import io.merak.etl.job.dispatcher.*;
import io.merak.etl.utils.classloader.*;
import com.google.common.base.*;
import io.merak.etl.extensions.exceptions.*;
import com.google.common.collect.*;
import java.util.*;
import java.util.stream.*;
import io.merak.etl.pipeline.dto.*;
import io.merak.etl.utils.config.*;
import io.merak.etl.executor.spark.task.*;
import io.merak.etl.executor.spark.task.extensions.*;
import io.merak.etl.executor.spark.translator.dto.*;
import merak.tools.ExceptionHandling.*;
import org.apache.spark.sql.*;
import io.merak.etl.sdk.task.*;
import io.merak.etl.sdk.pipeline.*;

public class CustomTargetTranslator extends SparkTranslator<CustomTargetNode> implements SinkTranslator
{
    private SparkCustomTarget customTarget;
    
    public CustomTargetTranslator(final CustomTargetNode node, final TranslatorContext translatorContext) {
        super((PipelineNode)node, translatorContext);
        try {
            this.LOGGER.debug("initializing custom extension {} properties {}", (Object)node.getClassName());
            Class transformationClass;
            if (PipelineUtils.getDispatcherJobType(this.requestContext) == JobType.NATIVE_JOB) {
                final ClassLoader loader = ExtensionUtils.getInstance().getClassLoader(node.getExtension(), node.getClassName());
                ExtensionUtils.getInstance().addJarsToSparkContext((PipelineNode)node, this.spark, this.requestContext);
                transformationClass = loader.loadClass(node.getClassName());
            }
            else {
                transformationClass = Class.forName(node.getClassName());
            }
            this.customTarget = transformationClass.newInstance();
        }
        catch (ExtensionsInitializationException e) {
            this.LOGGER.error(e.getMessage());
            Throwables.propagate((Throwable)e);
        }
        catch (ClassNotFoundException e2) {
            final String error = String.format("Class %s not found .Please ensure jars are registered in infoworks platform!!", node.getClassName());
            this.LOGGER.error(error);
            Throwables.propagate((Throwable)e2);
        }
        catch (IllegalAccessException | InstantiationException ex2) {
            final ReflectiveOperationException ex;
            final ReflectiveOperationException e3 = ex;
            final String error = String.format("Class %s does not have a null constructor . Please ensure to have a null constructor ", node.getClassName());
            this.LOGGER.error(error);
            Throwables.propagate((Throwable)e3);
        }
    }
    
    @Override
    protected void generateDataFrame() {
        try {
            if (PipelineUtils.getDispatcherJobType(this.requestContext) == JobType.NATIVE_JOB) {
                ExtensionUtils.getInstance().addClassLoader(((CustomTargetNode)this.node).getExtension());
            }
            final Dataset<Row> inputDF = this.getDataset(this.getParentNode((PipelineNode)this.node));
            final List<String> plan = (List<String>)Lists.newArrayList();
            plan.add(String.format("Node : %s", ((CustomTargetNode)this.node).getId()));
            final List<String> selectCols = (List<String>)((CustomTargetNode)this.node).getOutputEntities().stream().map(i -> String.format("%s AS %s", i.getInputName(), i.getOutputName(i.getOwnerNode()))).collect(Collectors.toList());
            this.LOGGER.debug("Target input columns :{} ", (Object)String.join(",", (CharSequence[])inputDF.columns()));
            final Dataset<Row> targetDF = (Dataset<Row>)this.remapInputDataset(inputDF, ((CustomTargetNode)this.node).getOutputEntities());
            this.LOGGER.debug("Target output columns :{} ", (Object)String.join(",", (CharSequence[])targetDF.columns()));
            plan.add(String.format("Columns :%s", String.join(",", selectCols)));
            final DataFrameObject targetDFO = new DataFrameObject(targetDF, String.join("\n", plan));
            this.sparkTranslatorState.addDataFrame(this.node, targetDFO);
        }
        catch (Exception e) {
            final String error = String.format("Error while processing custom transformation %s", ((CustomTargetNode)this.node).getClassName());
            this.LOGGER.error(error, (Throwable)e);
            this.LOGGER.error("Exception: {}", (Object)AwbUtil.getErrorMessage((Throwable)e));
            IWRunTimeException.propagate(e, "EXTENSIONS_EXECUTION_ERROR");
        }
    }
    
    public SparkCustomTarget getCustomTarget() {
        return this.customTarget;
    }
    
    public CustomTargetNode getCustomTargetNode() {
        return (CustomTargetNode)this.node;
    }
    
    @Override
    public TaskNode translate() {
        super.translate();
        TaskNode taskNode = null;
        switch (this.requestContext.getJobType()) {
            case AWB_INTERACTIVE: {
                if (this.requestContext.getProcessingContext().isValidationMode()) {
                    taskNode = (TaskNode)new SparkValidatorTaskNode((SinkTranslator)this, (EndVertex)this.node, this.sparkTranslatorState);
                    break;
                }
                if (this.requestContext.getProcessingContext().isSchemaMode()) {
                    taskNode = (TaskNode)new SparkSchemaTaskNode((SinkTranslator)this, (EndVertex)this.node, this.sparkTranslatorState);
                    break;
                }
                taskNode = (TaskNode)new SparkCustomTargetInteractiveTaskNode(this, (CustomTargetNode)this.node, this.sparkTranslatorState);
                break;
            }
            case AWB_BATCH: {
                taskNode = (TaskNode)new SparkCustomTargetBatchTaskNode(this, (CustomTargetNode)this.node, this.sparkTranslatorState);
                break;
            }
            default: {
                IWRunTimeException.propagate((Exception)new RuntimeException(" invalid job type" + this.requestContext.getJobType()), "CUSTOM_TARGET_TRANSLATE_ERROR");
                break;
            }
        }
        return taskNode;
    }
    
    public SparkTranslatorState getSparkTranslatorState() {
        return this.sparkTranslatorState;
    }
}
