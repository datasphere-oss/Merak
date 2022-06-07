package io.merak.etl.executor.hive.translator;

import java.util.function.*;
import java.util.stream.*;

import com.google.common.collect.*;

import io.merak.etl.pipeline.dto.*;
import io.merak.etl.pipeline.dto.pivot.*;
import io.merak.etl.sdk.task.*;
import io.merak.etl.translator.*;

import com.google.common.base.*;
import java.util.*;

public class PivotTranslator extends SqlTranslator
{
    protected final PivotNode node;
    
    public PivotTranslator(final PivotNode node, final TranslatorContext translatorContext) {
        super(translatorContext);
        this.node = node;
    }
    
    @Override
    protected boolean canUseStarInSelect(final Function<Boolean, Stream<Entity>> entitiesUsedFormatter) {
        return false;
    }
    
    @Override
    protected void end() {
        if (this.node.getGroupBys() != null && this.node.getGroupBys().size() > 0) {
            final String expressions = (String)this.node.getGroupBys().stream().map(Derivation::getExpression).collect(Collectors.joining(", "));
            this.builder.append(" GROUP BY ").append(expressions);
        }
        this.appendPostActions((PipelineNode)this.node);
        super.end();
    }
    
    public TaskNode translate() {
        String parent = this.getParent((PipelineNode)this.node);
        parent = this.appendTemporaryDerivations((PipelineNode)this.node, parent);
        this.appendCaseSelectStatements(parent);
        final Map<Entity, String> columnToExpressionMap = (Map<Entity, String>)Maps.newHashMap();
        if (this.node.getGroupBys().size() > 0) {
            final String s;
            this.node.getGroupBys().forEach(i -> s = columnToExpressionMap.put(i.getDerivedFields().get(0), i.getExpression()));
        }
        for (final Derivation<CustomAttributes> outputDerivation : this.node.getPivotOutputs()) {
            final PivotOutputAttributes pivotOutputAttributes = (PivotOutputAttributes)outputDerivation.getCustomAttributes();
            final PivotAggregation aggregation = this.node.getPivotAggregation(pivotOutputAttributes.getPivotAggregationId());
            final String aggregatedColumnId = this.getAliasNameForDerivedField(outputDerivation);
            columnToExpressionMap.put(outputDerivation.getDerivedField(), aggregation.getExpression(aggregatedColumnId));
        }
        final String expression;
        this.start(this.node.getId()).select((PipelineNode)this.node, column -> {
            expression = columnToExpressionMap.get(column);
            Preconditions.checkNotNull((Object)expression, "No expression found for column %s!", new Object[] { column.getGivenName() });
            return String.format("%s AS %s", expression, column.getOutputName((PipelineNode)this.node));
        }).from(this.getPreprocessName());
        this.end();
        return null;
    }
    
    private void appendCaseSelectStatements(final String parent) {
        final String caseFormat = "case when %s then %s end as %s";
        final Map<Entity, String> columnToExpressionMap = (Map<Entity, String>)Maps.newHashMap();
        final Column pivotColumn = (Column)this.node.getInputOrTempEntityByName(this.node.getPivotConfiguration().getColumn());
        this.node.getGroupBys().forEach(i -> columnToExpressionMap.put(i.getDerivedField(), i.getExpression()));
        for (final Derivation<CustomAttributes> outputDerivation : this.node.getPivotOutputs()) {
            final PivotOutputAttributes pivotOutputAttributes = (PivotOutputAttributes)outputDerivation.getCustomAttributes();
            final PivotValue pivotValue = this.node.getPivotValue(pivotOutputAttributes.getPivotValueId());
            final PivotAggregation aggregation = this.node.getPivotAggregation(pivotOutputAttributes.getPivotAggregationId());
            final String aggregationColumn = this.node.getInputOrTempEntityByName(aggregation.getAggColumn()).getOutputName();
            final String predicate = String.format("CAST(%s AS STRING) = '%s'", pivotColumn.getInputName(), pivotValue.getValue());
            final String caseStatement = String.format(caseFormat, predicate, aggregationColumn, this.getAliasNameForDerivedField(outputDerivation));
            columnToExpressionMap.put(outputDerivation.getDerivedField(), caseStatement);
        }
        final String expression;
        this.start(this.getPreprocessName()).select((PipelineNode)this.node, column -> {
            expression = columnToExpressionMap.get(column);
            Preconditions.checkNotNull((Object)expression, "No expression found for column %s!", new Object[] { column.getGivenName() });
            return expression;
        }).from(parent).end(this.builder);
    }
    
    private String getAliasNameForDerivedField(final Derivation<CustomAttributes> outputDerivation) {
        return outputDerivation.getDerivedField().getOutputName();
    }
    
    private String getPreprocessName() {
        return this.node.getId() + "_preprocess";
    }
}
