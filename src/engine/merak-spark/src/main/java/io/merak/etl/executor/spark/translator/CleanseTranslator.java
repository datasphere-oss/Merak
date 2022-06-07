package io.merak.etl.executor.spark.translator;

import io.merak.etl.translator.*;
import io.merak.etl.executor.spark.translator.dto.*;
import io.merak.etl.executor.spark.utils.*;

import java.util.function.*;
import java.util.stream.*;
import io.merak.etl.pipeline.dto.*;
import com.google.common.collect.*;

import java.util.*;

import org.apache.spark.sql.*;

public class CleanseTranslator extends SparkTranslator<CleanseNode>
{
    private boolean cleaningColumns;
    
    public CleanseTranslator(final CleanseNode node, final TranslatorContext translatorContext) {
        super((PipelineNode)node, translatorContext);
    }
    
    @Override
    protected boolean canUseStarInSelect(final Function<Boolean, Stream<Entity>> entitiesUsedFormatter) {
        return !this.cleaningColumns && super.canUseStarInSelect(entitiesUsedFormatter);
    }
    
    @Override
    protected void generateDataFrame() {
        final List<String> plan = (List<String>)Lists.newArrayList();
        plan.add(String.format("Node %s", ((CleanseNode)this.node).getName()));
        this.cleaningColumns = true;
        final PipelineNode parent = this.getParentNode((PipelineNode)this.node);
        final DataFrameObject incomingDFO = this.sparkTranslatorState.getDataFrame(parent);
        Dataset<Row> cleansedDataSet = incomingDFO.getDataset();
        cleansedDataSet = (Dataset<Row>)cleansedDataSet.as(((CleanseNode)this.node).getId());
        int i = 0;
        for (final CleanseNode.Transformation t : ((CleanseNode)this.node).getTransformations()) {
            if (!this.requestContext.getProcessingContext().isValidationMode() && !((CleanseNode)this.node).isTransformationUsed(t)) {
                continue;
            }
            if (!(t instanceof CleanseNode.LookupTransformation)) {
                throw new UnsupportedOperationException("Unknown transformation " + t);
            }
            cleansedDataSet = this.translate(cleansedDataSet, (CleanseNode.LookupTransformation)t);
            final Map<String, String> transformation = (Map<String, String>)Maps.newHashMap();
            transformation.put("Lookup Table Name", ((CleanseNode.LookupTransformation)t).getLookupTableName());
            transformation.put("Cleanup Column", ((CleanseNode.LookupTransformation)t).getCleanupColumn().getOutputName((PipelineNode)this.node));
            transformation.put("Lookup Column", ((CleanseNode.LookupTransformation)t).getLookupColumnName());
            transformation.put("Value Column Name", ((CleanseNode.LookupTransformation)t).getValueColumnName());
            plan.add(String.format("Transformation %s with %s", i, transformation.toString()));
            ++i;
        }
        this.cleaningColumns = false;
        this.sparkTranslatorState.addPlan(this.node, plan);
        cleansedDataSet = this.appendDerivations(this.node, cleansedDataSet, plan);
        cleansedDataSet = this.select(this.node, cleansedDataSet);
        final DataFrameObject cleansedDFO = new DataFrameObject(cleansedDataSet, String.join("\n", plan));
        this.sparkTranslatorState.addDataFrame(this.node, cleansedDFO);
    }
    
    private Dataset<Row> translate(Dataset<Row> cleansedDataSet, final CleanseNode.LookupTransformation transformation) {
        final String expression = String.format("%s.%s = %s.%s", transformation.getLookupTableName(), transformation.getLookupColumnName(), ((CleanseNode)this.node).getId(), transformation.getCleanupColumn().getOutputName((PipelineNode)this.node));
        final Column joinCondition = SparkUtils.getColumnFromExp(this.spark, expression);
        final String lookUpTableQuery = String.format("select * from `%s`.`%s`", transformation.getLookupSchemaName(), transformation.getLookupTableName());
        final Dataset<Row> lookUpTable = (Dataset<Row>)this.spark.sql(lookUpTableQuery);
        cleansedDataSet = (Dataset<Row>)cleansedDataSet.as(((CleanseNode)this.node).getId()).join(lookUpTable.as(transformation.getLookupTableName()), joinCondition, "leftouter");
        cleansedDataSet = this.select((PipelineNode)this.node, c -> {
            if (c.getId().equals(transformation.getCleanupColumn().getId())) {
                return String.format("`%s`.`%s` AS %s", transformation.getLookupTableName(), transformation.getValueColumnName(), c.getOutputName((PipelineNode)this.node));
            }
            else {
                return String.format("%s.%s", ((CleanseNode)this.node).getId(), c.getOutputName((PipelineNode)this.node));
            }
        }, cleansedDataSet, c -> ((CleanseNode)this.node).getInputEntitiesUsed(this.requestContext.getProcessingContext().isValidationMode()));
        return cleansedDataSet;
    }
}
