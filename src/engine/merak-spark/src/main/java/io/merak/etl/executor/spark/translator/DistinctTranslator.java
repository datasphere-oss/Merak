package io.merak.etl.executor.spark.translator;

import io.merak.etl.translator.*;
import io.merak.etl.executor.spark.translator.dto.*;

import java.util.function.*;
import io.merak.etl.pipeline.dto.*;
import com.google.common.collect.*;
import java.util.stream.*;
import java.util.*;
import org.apache.spark.sql.*;

public class DistinctTranslator extends SparkTranslator<DistinctNode>
{
    public DistinctTranslator(final DistinctNode node, final TranslatorContext translatorContext) {
        super((PipelineNode)node, translatorContext);
    }
    
    @Override
    protected boolean canUseStarInSelect(final Function<Boolean, Stream<Entity>> entitiesUsedFormatter) {
        return false;
    }
    
    @Override
    protected void generateDataFrame() {
        final List<String> plan = (List<String>)Lists.newArrayList();
        plan.add(String.format("Node : %s", ((DistinctNode)this.node).getId()));
        final PipelineNode parent = this.getParentNode((PipelineNode)this.node);
        final DataFrameObject incomingDFO = this.sparkTranslatorState.getDataFrame(parent);
        Dataset<Row> distinctSet = incomingDFO.getDataset();
        distinctSet = this.appendDerivations(this.node, distinctSet, plan);
        plan.add(String.format("Distinct Column List {%s}", String.join(",", (Iterable<? extends CharSequence>)((DistinctNode)this.node).getOutputEntities().stream().map(i -> i.getGivenName()).collect(Collectors.toList()))));
        distinctSet = (Dataset<Row>)this.select(this.node, distinctSet).distinct();
        distinctSet = this.appendPostActions(this.node, distinctSet, plan);
        this.sparkTranslatorState.addPlan(this.node, plan);
        final DataFrameObject distinctDFO = new DataFrameObject(distinctSet, String.join("\n", plan));
        this.sparkTranslatorState.addDataFrame(this.node, distinctDFO);
    }
}
