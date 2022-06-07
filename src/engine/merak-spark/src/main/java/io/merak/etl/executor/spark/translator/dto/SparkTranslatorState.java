package io.merak.etl.executor.spark.translator.dto;

import io.merak.etl.sdk.translator.*;
import io.merak.etl.executor.spark.session.*;

import org.apache.spark.sql.*;

import io.merak.etl.context.*;
import org.slf4j.*;
import java.util.*;
import com.google.common.collect.*;
import io.merak.etl.pipeline.dto.*;

public class SparkTranslatorState implements TranslatorState<SparkTranslatorState>
{
    private Map<String, DataFrameObject> dataFrameObjectMap;
    private List<String> logicalPlan;
    private List<Translatable> translations;
    private int stepCount;
    SparkSession spark;
    DFSparkSession dfSparkSession;
    protected final Logger LOGGER;
    
    public SparkTranslatorState(final RequestContext requestContext) {
        this.translations = (List<Translatable>)Lists.newArrayList();
        this.stepCount = 1;
        this.LOGGER = LoggerFactory.getLogger((Class)this.getClass());
        this.dataFrameObjectMap = new HashMap<String, DataFrameObject>();
        this.logicalPlan = new ArrayList<String>();
        this.dfSparkSession = DFSparkSession.getInstance(requestContext);
        this.spark = this.dfSparkSession.getSpark();
    }
    
    public void addDataFrame(final PipelineNode node, final DataFrameObject dataFrameObject) {
        this.dataFrameObjectMap.put(node.getId(), dataFrameObject);
        this.addLogicalPlan(dataFrameObject);
    }
    
    public DataFrameObject getDataFrame(final PipelineNode node) {
        return this.dataFrameObjectMap.get(node.getId());
    }
    
    public SparkTranslatorState get() {
        return this;
    }
    
    public void addPlan(final PipelineNode node, final List<String> plan) {
        final StringBuilder columns = new StringBuilder();
        columns.append("Columns :");
        node.getOutputEntities().forEach(i -> columns.append(String.format("%s:%s ", i.getGivenName(), i.getInputName())));
        plan.add(columns.toString());
        plan.forEach(i -> this.LOGGER.debug("Node {} Step {} : {} ", new Object[] { node.getName(), this.stepCount, i }));
        this.logicalPlan.addAll(plan);
    }
    
    public void addLogicalPlan(final DataFrameObject dataFrameObject) {
        final String planStep;
        dataFrameObject.getLogicalPlan().forEach(i -> {
            planStep = String.format("Step %d : %s", this.stepCount, i);
            this.logicalPlan.add(planStep);
            return;
        });
        ++this.stepCount;
    }
    
    public List<String> getLogicalPlan() {
        return (List<String>)ImmutableList.copyOf((Collection)this.logicalPlan);
    }
    
    public SparkSession getSpark() {
        return this.spark;
    }
    
    public void addTranslations(final Translatable translatable) {
        this.translations.add(translatable);
    }
    
    public void runTranslations() {
        this.LOGGER.debug("executing all {} translations ", (Object)this.translations.size());
        this.translations.forEach(i -> i.executeTranslation());
    }
}
