package io.merak.etl.executor.hive.translator.previewsql;

import io.merak.etl.executor.hive.task.*;
import io.merak.etl.pipeline.dto.*;
import io.merak.etl.pipeline.dto.pivot.*;
import io.merak.etl.sdk.task.*;
import io.merak.etl.translator.*;

import java.util.function.*;
import java.util.stream.*;

import com.google.common.collect.*;
import com.google.common.base.*;

import java.util.*;

public class PivotTranslator extends NodeSqlTranslator
{
    protected final PivotNode node;
    
    public PivotTranslator(final PivotNode node, final TranslatorContext translatorContext) {
        super(translatorContext);
        this.node = node;
    }
    
    @Override
    protected void end() {
        if (this.node.getGroupBys() != null && this.node.getGroupBys().size() > 0) {
            final String expressions = (String)this.node.getGroupBys().stream().map(Derivation::getOriginalExpression).collect(Collectors.joining(", "));
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
            this.node.getGroupBys().forEach(i -> s = columnToExpressionMap.put(i.getDerivedFields().get(0), i.getOriginalExpression()));
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
            return String.format("%s as `%s`", expression, column.getGivenName());
        }).from(this.getPreprocessName());
        this.end();
        return (TaskNode)new QueryTaskNode(this.builder.toString());
    }
    
    private void appendCaseSelectStatements(final String parent) {
        final String caseFormat = "case when %s then %s end as %s";
        final Map<Entity, String> columnToExpressionMap = (Map<Entity, String>)Maps.newHashMap();
        this.node.getGroupBys().forEach(i -> columnToExpressionMap.put(i.getDerivedField(), i.getOriginalExpression()));
        for (final Derivation<CustomAttributes> outputDerivation : this.node.getPivotOutputs()) {
            final PivotOutputAttributes pivotOutputAttributes = (PivotOutputAttributes)outputDerivation.getCustomAttributes();
            final PivotValue pivotValue = this.node.getPivotValue(pivotOutputAttributes.getPivotValueId());
            final PivotAggregation aggregation = this.node.getPivotAggregation(pivotOutputAttributes.getPivotAggregationId());
            final String aggregationColumn = this.node.getInputOrTempEntityByName(aggregation.getAggColumn()).getGivenName();
            final String predicate = String.format("CAST(%s AS STRING) = '%s'", this.node.getPivotConfiguration().getColumn(), pivotValue.getValue());
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
        return outputDerivation.getDerivedField().getGivenName();
    }
    
    private String getPreprocessName() {
        return this.node.getId() + "_preprocess";
    }
}
