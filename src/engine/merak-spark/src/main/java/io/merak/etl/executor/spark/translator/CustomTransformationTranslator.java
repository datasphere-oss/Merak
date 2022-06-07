package io.merak.etl.executor.spark.translator;

import io.merak.etl.translator.*;
import io.merak.etl.dispatcher.job.*;
import io.merak.etl.job.dispatcher.*;
import io.merak.etl.utils.classloader.*;
import io.merak.etl.extensions.api.spark.*;
import io.merak.etl.utils.config.*;
import io.merak.etl.executor.spark.translator.dto.*;
import merak.tools.ExceptionHandling.*;
import io.merak.etl.pipeline.dto.*;
import com.google.common.collect.*;

import org.apache.spark.sql.*;
import io.merak.etl.extensions.exceptions.*;
import org.apache.spark.sql.types.*;
import java.util.*;

public class CustomTransformationTranslator extends SparkTranslator<CustomTransformationNode>
{
    private SparkCustomTransformation transformation;
    
    public CustomTransformationTranslator(final CustomTransformationNode node, final TranslatorContext translatorContext) {
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
            this.transformation = transformationClass.newInstance();
            final Map<String, Object> processingContext = (Map<String, Object>)Maps.newHashMap();
            processingContext.put("validation", this.requestContext.getProcessingContext().isValidationMode());
            this.transformation.initialiseContext(this.spark, node.getUserProperties(), new ProcessingContext((Map)processingContext));
        }
        catch (ExtensionsInitializationException e) {
            this.LOGGER.error(e.getMessage());
            this.LOGGER.error("Exception: {}", (Object)AwbUtil.getErrorMessage((Throwable)e));
            IWRunTimeException.propagate((Exception)e, "EXTENSIONS_INITIALIZER_ERROR");
        }
        catch (ClassNotFoundException e2) {
            final String error = String.format("Class %s not found .Please ensure jars are registered in infoworks platform!!", node.getClassName());
            this.LOGGER.error(error);
            this.LOGGER.error("Exception: {}", (Object)AwbUtil.getErrorMessage((Throwable)e2));
            IWRunTimeException.propagate((Exception)e2, "EXTENSIONS_INITIALIZER_ERROR");
        }
        catch (IllegalAccessException | InstantiationException ex2) {
            final ReflectiveOperationException ex;
            final ReflectiveOperationException e3 = ex;
            final String error = String.format("Class %s does not have a null constructor . Please ensure to have a null constructor ", node.getClassName());
            this.LOGGER.error(error);
            this.LOGGER.error("Exception: {}", (Object)AwbUtil.getErrorMessage((Throwable)e3));
            IWRunTimeException.propagate((Exception)e3, "EXTENSIONS_INITIALIZER_ERROR");
        }
    }
    
    @Override
    protected void generateDataFrame() {
        final Map<String, Dataset> inputMap = (Map<String, Dataset>)Maps.newHashMap();
        final Dataset inputDataset;
        final List<Entity> entityList;
        final Dataset inputDataset2;
        final Map<String, Dataset> map;
        ((CustomTransformationNode)this.node).getInputNodesToComponentsMap().forEach((parentNode, components) -> {
            inputDataset = this.getDataset(parentNode);
            entityList = new ArrayList<Entity>();
            entityList.addAll(components);
            inputDataset2 = this.remapInputDataset(inputDataset, entityList);
            map.put(parentNode.getName(), inputDataset2);
            return;
        });
        try {
            if (PipelineUtils.getDispatcherJobType(this.requestContext) == JobType.NATIVE_JOB) {
                ExtensionUtils.getInstance().addClassLoader(((CustomTransformationNode)this.node).getExtension());
            }
            Dataset outputDataset = this.transformation.transform((Map)inputMap);
            final List<String> plan = (List<String>)Lists.newArrayList();
            plan.add(String.format("Custom transformation %s  class : %s being executed", ((CustomTransformationNode)this.node).getName(), ((CustomTransformationNode)this.node).getClassName()));
            outputDataset = this.remapOutputDataset(outputDataset, ((CustomTransformationNode)this.node).getOutputEntities());
            final StructType outputSchema = outputDataset.schema();
            plan.add(String.format("Custom transformation output schema %s", outputSchema));
            this.sparkTranslatorState.addDataFrame(this.node, new DataFrameObject((Dataset<Row>)outputDataset, plan.toString()));
        }
        catch (ExtensionsProcessingException e) {
            final String error = String.format("Error while processing custom transformation %s", ((CustomTransformationNode)this.node).getClassName());
            this.LOGGER.error(error, (Throwable)e);
            this.LOGGER.error("Exception: {}", (Object)AwbUtil.getErrorMessage((Throwable)e));
            IWRunTimeException.propagate((Exception)e, "EXTENSIONS_EXECUTION_ERROR");
        }
    }
}
