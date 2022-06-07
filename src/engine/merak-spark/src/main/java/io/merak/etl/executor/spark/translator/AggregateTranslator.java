package io.merak.etl.executor.spark.translator;

import io.merak.etl.translator.*;
import io.merak.etl.executor.spark.translator.dto.*;
import io.merak.etl.executor.spark.utils.*;

import com.google.common.collect.*;
import io.merak.etl.pipeline.dto.*;
import scala.collection.*;
import com.google.common.base.*;

import org.apache.spark.sql.*;
import java.util.*;
import java.util.function.*;
import java.util.stream.*;

public class AggregateTranslator extends SparkTranslator<AggregateNode>
{
    public AggregateTranslator(final AggregateNode node, final TranslatorContext translatorContext) {
        super((PipelineNode)node, translatorContext);
    }
    
    @Override
    protected void generateDataFrame() {
        final List<String> plan = (List<String>)Lists.newArrayList();
        final Map<Entity, String> columnToExpressionMap = (Map<Entity, String>)Maps.newHashMap();
        final List<Entity> groupByColumns = (List<Entity>)Lists.newArrayList();
        final List<Entity> aggColumns = (List<Entity>)Lists.newArrayList();
        if (((AggregateNode)this.node).getAggregates().size() > 0) {
            final String s;
            ((AggregateNode)this.node).getAggregates().forEach(i -> s = columnToExpressionMap.put(i.getDerivedFields().get(0), i.getExpression()));
            ((AggregateNode)this.node).getAggregates().forEach(i -> aggColumns.add((Entity)i.getDerivedFields().get(0)));
        }
        if (((AggregateNode)this.node).getGroupBys().size() > 0) {
            final String s2;
            ((AggregateNode)this.node).getGroupBys().forEach(i -> s2 = columnToExpressionMap.put(i.getDerivedFields().get(0), i.getExpression()));
            ((AggregateNode)this.node).getGroupBys().forEach(i -> groupByColumns.add((Entity)i.getDerivedFields().get(0)));
        }
        plan.add(String.format("Node : %s", ((AggregateNode)this.node).getId()));
        final PipelineNode parent = this.getParentNode((PipelineNode)this.node);
        final DataFrameObject incomingDFO = this.sparkTranslatorState.getDataFrame(parent);
        Dataset<Row> aggregateSet = incomingDFO.getDataset();
        String aggregateExpression = null;
        aggregateSet = this.appendDerivations(this.node, aggregateSet, plan);
        this.LOGGER.debug("Schema: {}", (Object)aggregateSet.schema().treeString());
        final List<String> groupByExpressions = (List<String>)Lists.newArrayList();
        final List<String> aggregateExpressions = (List<String>)Lists.newArrayList();
        String expressions = "";
        if (((AggregateNode)this.node).getGroupBys() != null && ((AggregateNode)this.node).getGroupBys().size() > 0) {
            for (final Derivation groupBy : ((AggregateNode)this.node).getGroupBys()) {
                this.LOGGER.debug(String.format("Derive Expression :%s and derive Column :%s ", groupBy.getExpression(), groupBy.getDerivedFieldName()));
                final String expression = String.format("%s AS %s", groupBy.getExpression(), groupBy.getDerivedField().getOutputName((PipelineNode)this.node));
                groupByExpressions.add(expression);
            }
        }
        boolean firstExpression = true;
        if (((AggregateNode)this.node).getAggregates().size() > 0) {
            this.LOGGER.debug("Found derive expression");
            for (final Derivation derive : ((AggregateNode)this.node).getAggregates()) {
                if (!this.isDeriveUsed(derive.getDerivedField())) {
                    continue;
                }
                this.LOGGER.debug(String.format("Derive Expression :%s and derive Column :%s ", derive.getExpression(), derive.getDerivedField()));
                if (firstExpression) {
                    aggregateExpression = String.format("%s AS %s", derive.getExpression(), derive.getDerivedField().getOutputName((PipelineNode)this.node));
                    firstExpression = false;
                    expressions = expressions + aggregateExpression + " ";
                }
                else {
                    final String aggExp = String.format("%s AS %s", derive.getExpression(), derive.getDerivedField().getOutputName((PipelineNode)this.node));
                    aggregateExpressions.add(aggExp);
                    expressions = expressions + aggExp + " ";
                }
            }
        }
        plan.add(String.format("GroupBy Expressions : %s", groupByExpressions.toString()));
        final List<String> allAggExpressions = (List<String>)Lists.newArrayList();
        allAggExpressions.add(aggregateExpression);
        allAggExpressions.addAll(aggregateExpressions);
        plan.add(String.format("Aggregate Expressions : %s", String.join(" , ", allAggExpressions)));
        if (!firstExpression) {
            this.LOGGER.debug("Aggregate Expressions exists");
            aggregateSet = (Dataset<Row>)aggregateSet.groupBy((Seq)SparkUtils.getColumnSeqFromExpressions(this.spark, groupByExpressions)).agg(SparkUtils.getColumnFromExp(this.spark, aggregateExpression), (Seq)SparkUtils.getColumnSeqFromExpressions(this.spark, aggregateExpressions));
        }
        else {
            this.LOGGER.debug("Aggregate Expressions does not exist");
            final Map<String, String> dummyAggregateExpression = (Map<String, String>)Maps.newHashMap();
            aggregateSet = (Dataset<Row>)aggregateSet.groupBy((Seq)SparkUtils.getColumnSeqFromExpressions(this.spark, groupByExpressions)).agg((Map)dummyAggregateExpression);
        }
        final String expression2;
        aggregateSet = this.select((PipelineNode)this.node, column -> {
            expression2 = columnToExpressionMap.get(column);
            Preconditions.checkNotNull((Object)expression2, "No expression found for column %s!", new Object[] { column.getGivenName() });
            return String.format("%s", column.getOutputName((PipelineNode)this.node));
        }, aggregateSet);
        aggregateSet = this.appendPostActions(this.node, aggregateSet, plan);
        this.sparkTranslatorState.addPlan(this.node, plan);
        final DataFrameObject aggregateDFO = new DataFrameObject(aggregateSet, String.join("\n", plan));
        this.sparkTranslatorState.addDataFrame(this.node, aggregateDFO);
    }
    
    @Override
    protected boolean canUseStarInSelect(final Function<Boolean, Stream<Entity>> entitiesUsedFormatter) {
        return false;
    }
    
    public boolean isDeriveUsed(final Entity column) {
        final List<Entity> columnsUsed = ((AggregateNode)this.node).getOutputEntitiesUsed(this.requestContext.getProcessingContext().isValidationMode()).collect(Collectors.toList());
        return columnsUsed.contains(column);
    }
}
