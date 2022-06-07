package io.merak.etl.executor.spark.translator;

import io.merak.etl.translator.*;
import io.merak.etl.executor.spark.translator.dto.*;
import io.merak.etl.executor.spark.utils.*;

import com.google.common.collect.*;

import java.util.*;
import org.apache.spark.sql.*;
import java.util.stream.*;
import io.merak.etl.pipeline.dto.*;

public class ExistsTranslator extends SparkTranslator<ExistsNode>
{
    public ExistsTranslator(final ExistsNode node, final TranslatorContext translatorContext) {
        super((PipelineNode)node, translatorContext);
    }
    
    @Override
    protected void generateDataFrame() {
        final List<String> plan = (List<String>)Lists.newArrayList();
        plan.add(String.format("Node %s :", ((ExistsNode)this.node).getId()));
        final PipelineNode innerTable = ((ExistsNode)this.node).getInnerTable();
        final DataFrameObject innerTableDFO = this.sparkTranslatorState.getDataFrame(innerTable);
        final Dataset<Row> innerTableSet = innerTableDFO.getDataset();
        plan.add(String.format("Inner Table Node %s :", innerTable.getId()));
        final PipelineNode outerTable = ((ExistsNode)this.node).getOuterTable();
        final DataFrameObject outerTableDFO = this.sparkTranslatorState.getDataFrame(outerTable);
        final Dataset<Row> outerTableSet = outerTableDFO.getDataset();
        plan.add(String.format("Outer Table Node %s :", outerTable.getId()));
        plan.add(String.format("Expression : %s", ((ExistsNode)this.node).getExpression()));
        this.LOGGER.debug(String.format("Expression : %s", ((ExistsNode)this.node).getExpression()));
        final Column joinCondition = SparkUtils.getColumnFromExp(this.spark, ((ExistsNode)this.node).getExpression());
        String joinType;
        if (((ExistsNode)this.node).getType().equals("EXISTS")) {
            joinType = "leftsemi";
        }
        else {
            joinType = "leftanti";
        }
        Dataset<Row> joinedSet = (Dataset<Row>)outerTableSet.as(outerTable.getId()).join(innerTableSet.as(innerTable.getId()), joinCondition, joinType);
        joinedSet = this.select((PipelineNode)this.node, entity -> String.format("%s.%s AS %s", entity.getReferencedEntity().getOwnerNode().getId(), entity.getInputName(), entity.getOutputName()), joinedSet, i -> ((ExistsNode)this.node).getInputEntitiesUsedByPortId(this.requestContext.getProcessingContext().isValidationMode(), ((ExistsNode)this.node).getOuterPortId()).stream());
        this.sparkTranslatorState.addPlan(this.node, plan);
        joinedSet = this.select(this.node, joinedSet);
        final DataFrameObject joinedDFO = new DataFrameObject(joinedSet, String.join("\n", plan));
        this.sparkTranslatorState.addDataFrame(this.node, joinedDFO);
    }
}
