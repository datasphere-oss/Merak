package io.merak.etl.executor.spark.translator;

import io.merak.etl.translator.*;
import io.merak.etl.executor.spark.translator.dto.*;
import io.merak.etl.executor.spark.utils.*;
import io.merak.etl.pipeline.dto.*;
import com.google.common.collect.*;
import com.google.common.base.*;

import org.apache.spark.sql.*;
import java.util.*;

public class FilterTranslator extends SparkTranslator<FilterNode>
{
    public FilterTranslator(final FilterNode node, final TranslatorContext translatorContext) {
        super((PipelineNode)node, translatorContext);
    }
    
    @Override
    protected void generateDataFrame() {
        final PipelineNode parent = this.getParentNode((PipelineNode)this.node);
        final DataFrameObject incomingDFO = this.sparkTranslatorState.getDataFrame(parent);
        String predicate = ((FilterNode)this.node).getExpression();
        Dataset<Row> filterSet = incomingDFO.getDataset();
        this.LOGGER.debug("Schema for node {}: {}", (Object)((FilterNode)this.node).getName(), (Object)filterSet.schema().treeString());
        final List<String> plan = (List<String>)Lists.newArrayList();
        plan.add(String.format("Node : %s", ((FilterNode)this.node).getId()));
        filterSet = this.appendDerivations(this.node, filterSet, plan);
        if (((FilterNode)this.node).hasAnalyticsFunctions()) {
            final String conditionColumn = "iw_" + ((FilterNode)this.node).getId() + "_condition";
            filterSet = (Dataset<Row>)filterSet.withColumn(conditionColumn, SparkUtils.getColumnFromExp(this.spark, predicate));
            predicate = conditionColumn + "=true";
        }
        if (!Strings.isNullOrEmpty(predicate)) {
            filterSet = (Dataset<Row>)filterSet.filter(SparkUtils.getColumnFromExp(this.spark, predicate));
        }
        filterSet = this.select(this.node, filterSet);
        plan.add(String.format("Filter Expression: %s", ((FilterNode)this.node).getExpression()));
        this.sparkTranslatorState.addPlan(this.node, plan);
        final DataFrameObject filterDFO = new DataFrameObject(filterSet, String.join("\n", plan));
        this.sparkTranslatorState.addDataFrame(this.node, filterDFO);
    }
}
