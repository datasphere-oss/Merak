package io.merak.etl.executor.spark.translator;

import io.merak.etl.translator.*;
import io.merak.etl.pipeline.dto.*;
import io.merak.etl.dispatcher.job.*;
import io.merak.etl.job.dispatcher.*;
import io.merak.etl.utils.classloader.*;
import io.merak.etl.executor.spark.translator.dto.*;
import io.merak.etl.extensions.api.spark.*;
import com.google.common.collect.*;
import org.apache.spark.sql.*;

import io.merak.etl.extensions.exceptions.*;
import java.util.*;

public class CustomSourceTranslator extends SparkTranslator<CustomSourceNode>
{
    private SparkCustomSource customSource;
    
    public CustomSourceTranslator(final CustomSourceNode node, final TranslatorContext translatorContext) {
        super((PipelineNode)node, translatorContext);
        try {
            Class transformationClass;
            if (PipelineUtils.getDispatcherJobType(this.requestContext) == JobType.NATIVE_JOB) {
                final ClassLoader loader = ExtensionUtils.getInstance().getClassLoader(node.getExtension(), node.getClassName());
                ExtensionUtils.getInstance().addJarsToSparkContext((PipelineNode)node, this.spark, this.requestContext);
                transformationClass = loader.loadClass(node.getClassName());
            }
            else {
                transformationClass = Class.forName(node.getClassName());
            }
            this.customSource = transformationClass.newInstance();
            final Map<String, Object> processingContext = (Map<String, Object>)Maps.newHashMap();
            this.customSource.initialiseContext(this.spark, node.getUserProperties(), new ProcessingContext((Map)processingContext));
        }
        catch (ExtensionsInitializationException e) {
            this.LOGGER.error(e.getMessage());
            throw new RuntimeException((Throwable)e);
        }
        catch (ClassNotFoundException e3) {
            final String error = String.format("Class %s not found .Please ensure jars are registered in infoworks platform!!", node.getClassName());
            this.LOGGER.error(error);
            throw new RuntimeException(error);
        }
        catch (IllegalAccessException | InstantiationException ex2) {
            final ReflectiveOperationException ex;
            final ReflectiveOperationException e2 = ex;
            final String error = String.format("Class %s does not have a null constructor . Please ensure to have a null constructor ", node.getClassName());
            this.LOGGER.error(error);
            throw new RuntimeException(error);
        }
    }
    
    @Override
    protected void generateDataFrame() {
        try {
            Dataset outputDataset = this.customSource.readFromSource();
            final List<String> plan = (List<String>)Lists.newArrayList();
            plan.add(String.format("Custom source %s  class : %s being executed", ((CustomSourceNode)this.node).getName(), ((CustomSourceNode)this.node).getClassName()));
            this.LOGGER.debug("selecting column..");
            outputDataset = this.select(this.node, (Dataset<Row>)outputDataset);
            this.sparkTranslatorState.addDataFrame(this.node, new DataFrameObject((Dataset<Row>)outputDataset, plan.toString()));
        }
        catch (ExtensionsProcessingException e) {
            final String error = String.format("Error while processing custom class %s", ((CustomSourceNode)this.node).getClassName());
            this.LOGGER.error(error, (Throwable)e);
            throw new RuntimeException(error);
        }
    }
}
