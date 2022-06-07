package io.merak.etl.executor.spark.translator;

import io.merak.etl.translator.*;
import io.merak.etl.executor.spark.translator.dto.*;
import io.merak.etl.executor.spark.utils.*;

import com.google.common.collect.*;

import scala.collection.*;

import org.apache.spark.sql.*;
import java.util.*;
import io.merak.etl.pipeline.dto.pivot.*;
import java.util.function.*;
import java.util.stream.*;
import io.merak.etl.pipeline.dto.*;

public class PivotTranslator extends SparkTranslator<PivotNode>
{
    private List<String> plan;
    private List<Object> pivotValues;
    
    public PivotTranslator(final PivotNode node, final TranslatorContext translatorContext) {
        super((PipelineNode)node, translatorContext);
        this.plan = (List<String>)Lists.newArrayList();
        this.pivotValues = (List<Object>)node.getPivotConfiguration().getPivotValues().stream().map(i -> i.getValue()).collect(Collectors.toList());
    }
    
    @Override
    protected void generateDataFrame() {
        this.plan.add(String.format("Node : %s", ((PivotNode)this.node).getId()));
        final PipelineNode parent = this.getParentNode((PipelineNode)this.node);
        final DataFrameObject incomingDFO = this.sparkTranslatorState.getDataFrame(parent);
        Dataset<Row> aggregateSet = incomingDFO.getDataset();
        aggregateSet = this.appendDerivations(this.node, aggregateSet, this.plan);
        final List<String> groupByExpressions = this.getGroupyByExpressions();
        final List<String> aggregateExpressions = this.getAggregateExpressions();
        final String pivotColumn = ((PivotNode)this.node).getPivotConfiguration().getColumn();
        this.plan.add(String.format("GroupBy Expressions : %s", groupByExpressions.toString()));
        this.plan.add(String.format("Pivot column : %s", pivotColumn));
        this.plan.add(String.format("Aggregate Expressions : %s", String.join(" , ", aggregateExpressions)));
        this.LOGGER.debug(String.format("GroupBy Expressions : %s", groupByExpressions.toString()));
        this.LOGGER.debug(String.format("Pivot column : %s %s", pivotColumn, ((PivotNode)this.node).getInputOrTempEntityByName(pivotColumn).getOutputName()));
        this.LOGGER.debug(String.format("Aggregate Expressions : %s", String.join(" , ", aggregateExpressions)));
        this.LOGGER.debug("Schema :{}", (Object)aggregateSet.schema().treeString());
        final RelationalGroupedDataset groupedSet = aggregateSet.groupBy((Seq)SparkUtils.getColumnSeqFromExpressions(this.spark, groupByExpressions)).pivot(((PivotNode)this.node).getInputOrTempEntityByName(pivotColumn).getOutputName(), (List)this.pivotValues);
        if (aggregateExpressions.size() == 1) {
            aggregateSet = (Dataset<Row>)groupedSet.agg(SparkUtils.getColumnFromExp(this.spark, aggregateExpressions.get(0)), new Column[0]);
        }
        else {
            aggregateSet = (Dataset<Row>)groupedSet.agg(SparkUtils.getColumnFromExp(this.spark, aggregateExpressions.get(0)), (Seq)SparkUtils.getColumnSeqFromExpressions(this.spark, aggregateExpressions.subList(1, aggregateExpressions.size())));
        }
        aggregateSet = (Dataset<Row>)this.renameAggregateSet(aggregateSet, aggregateExpressions);
        this.sparkTranslatorState.addPlan(this.node, this.plan);
        final DataFrameObject aggregateDFO = new DataFrameObject(aggregateSet, String.join("\n", this.plan));
        this.sparkTranslatorState.addDataFrame(this.node, aggregateDFO);
    }
    
    private List<String> getGroupyByExpressions() {
        final List<String> groupByExpressions = (List<String>)Lists.newArrayList();
        if (((PivotNode)this.node).getGroupBys() != null && ((PivotNode)this.node).getGroupBys().size() > 0) {
            for (final Derivation groupBy : ((PivotNode)this.node).getGroupBys()) {
                this.LOGGER.debug(String.format("Derive Expression :%s and derive Column :%s ", groupBy.getExpression(), groupBy.getDerivedFieldName()));
                groupByExpressions.add(groupBy.getExpression());
            }
        }
        return groupByExpressions;
    }
    
    private List<String> getAggregateExpressions() {
        final List<String> aggregateExpressions = (List<String>)Lists.newArrayList();
        for (final PivotAggregation pivotAggregation : ((PivotNode)this.node).getAggregates()) {
            final String aggregationSystemColumn = ((PivotNode)this.node).getInputOrTempEntityByName(pivotAggregation.getAggColumn()).getOutputName();
            final String aggregateExpression = pivotAggregation.getExpression(aggregationSystemColumn);
            this.LOGGER.debug(String.format("Aggregate Expression :%s  ", aggregateExpression));
            aggregateExpressions.add(aggregateExpression);
        }
        return aggregateExpressions;
    }
    
    private Dataset renameAggregateSet(Dataset aggregateSet, final List<String> aggregateExpressions) {
        this.LOGGER.debug("Current schema " + aggregateSet.schema());
        final int numOfAggregates = aggregateExpressions.size();
        if (numOfAggregates == 1) {
            for (final Derivation<CustomAttributes> derivation : ((PivotNode)this.node).getPivotOutputs()) {
                final PivotOutputAttributes outputAttributes = (PivotOutputAttributes)derivation.getCustomAttributes();
                final PivotValue pivotValue = ((PivotNode)this.node).getPivotValue(outputAttributes.getPivotValueId());
                this.plan.add(String.format("Renaming spark column from %s to %s", pivotValue.getValue(), derivation.getDerivedField().getInputName()));
                aggregateSet = aggregateSet.withColumnRenamed(pivotValue.getValue(), derivation.getDerivedField().getInputName());
            }
        }
        else {
            final String[] fieldNames = aggregateSet.schema().fieldNames();
            int currentFieldIndex = ((PivotNode)this.node).getGroupBys().size();
            for (final PivotValue pivotValue : ((PivotNode)this.node).getPivotConfiguration().getPivotValues()) {
                for (final PivotAggregation aggregation : ((PivotNode)this.node).getAggregates()) {
                    final Derivation<CustomAttributes> derivation2 = (Derivation<CustomAttributes>)((PivotNode)this.node).getDerivation(aggregation.getId(), pivotValue.getId());
                    if (derivation2 != null) {
                        this.LOGGER.trace("Renaming {} to {}", (Object)fieldNames[currentFieldIndex], (Object)derivation2.getDerivedField().getOutputName());
                        aggregateSet = aggregateSet.withColumnRenamed(fieldNames[currentFieldIndex], derivation2.getDerivedField().getOutputName());
                    }
                    ++currentFieldIndex;
                }
            }
        }
        for (final Derivation<CustomAttributes> groupBy : ((PivotNode)this.node).getGroupBys()) {
            this.LOGGER.debug("Renaming group by column {} to {}", (Object)groupBy.getExpression(), (Object)groupBy.getDerivedField().getOutputName((PipelineNode)this.node));
            aggregateSet = aggregateSet.withColumnRenamed(groupBy.getExpression(), groupBy.getDerivedField().getOutputName((PipelineNode)this.node));
        }
        this.LOGGER.debug("Renamed schema " + aggregateSet.schema());
        return aggregateSet;
    }
    
    @Override
    protected boolean canUseStarInSelect(final Function<Boolean, Stream<Entity>> entitiesUsedFormatter) {
        return false;
    }
}
