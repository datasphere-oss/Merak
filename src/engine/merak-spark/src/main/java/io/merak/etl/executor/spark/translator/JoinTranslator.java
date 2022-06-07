package io.merak.etl.executor.spark.translator;

import java.util.*;
import com.google.common.collect.*;

import org.apache.spark.sql.*;
import io.merak.etl.translator.*;
import io.merak.etl.executor.spark.translator.dto.*;
import io.merak.etl.executor.spark.utils.*;

import java.util.stream.*;
import io.merak.etl.pipeline.dto.*;

public class JoinTranslator extends SparkTranslator<JoinNode>
{
    public JoinTranslator(final JoinNode node, final TranslatorContext translatorContext) {
        super((PipelineNode)node, translatorContext);
    }
    
    public String generateExpression(final JoinNode.RightPort rightTable) {
        if (rightTable.getMode().equals((Object)PipelineNode.Mode.ADVANCED)) {
            return rightTable.getExpression();
        }
        final StringBuilder builder = new StringBuilder();
        for (final JoinNode.RightPort.JoinColumn joinColumn : rightTable.getJoinColumns()) {
            builder.append(String.format("%s.%s = %s.%s AND ", ((JoinNode)this.node).getLeftTable().getId(), joinColumn.getLeftColumn().getInputName(), rightTable.getRightTable().getId(), joinColumn.getRightColumn().getInputName()));
        }
        builder.setLength(builder.length() - 5);
        return builder.toString();
    }
    
    public Dataset<Row> getBroadcastSet(final Dataset<Row> dataset) {
        return (Dataset<Row>)functions.broadcast((Dataset)dataset);
    }
    
    public Dataset<Row> getBroadcastEnabledDataset(final Dataset<Row> dataset, final PipelineNode table, final List<String> plan) {
        final Dataset<Row> joinedSet = (Dataset<Row>)dataset.as(table.getId());
        Dataset<Row> res;
        if (((JoinNode)this.node).getBroadcastPortIds().contains(table.getId())) {
            res = this.getBroadcastSet(joinedSet);
            plan.add(String.format("Broadcast port: %s", table.getName()));
        }
        else {
            res = joinedSet;
        }
        return res;
    }
    
    @Override
    protected void generateDataFrame() {
        final List<String> plan = (List<String>)Lists.newArrayList();
        plan.add(String.format("Node : %s", ((JoinNode)this.node).getId()));
        final PipelineNode leftTable = ((JoinNode)this.node).getLeftTable();
        final DataFrameObject leftTableDFO = this.sparkTranslatorState.getDataFrame(leftTable);
        final Dataset<Row> leftTableSet = leftTableDFO.getDataset();
        plan.add("LeftTable : " + leftTable.getName());
        Dataset<Row> joinedSet = this.getBroadcastEnabledDataset(leftTableSet, leftTable, plan);
        for (final JoinNode.RightPort rightTable : ((JoinNode)this.node).getRightPorts()) {
            final DataFrameObject rightTableDFO = this.sparkTranslatorState.getDataFrame(rightTable.getRightTable());
            final Dataset<Row> rightTableSet = rightTableDFO.getDataset();
            if (!rightTable.getJoinType().equalsIgnoreCase("CROSS")) {
                final Column joinCondition = SparkUtils.getColumnFromExp(this.spark, this.generateExpression(rightTable));
                joinedSet = (Dataset<Row>)joinedSet.join((Dataset)this.getBroadcastEnabledDataset(rightTableSet, rightTable.getRightTable(), plan), joinCondition, rightTable.getJoinType().replaceAll(" ", ""));
                plan.add(String.format("Right table %s , Join Type : %s , Join condition : %s", rightTable.getRightTable().getName(), rightTable.getJoinType(), this.generateExpression(rightTable)));
            }
            else {
                joinedSet = (Dataset<Row>)joinedSet.join((Dataset)this.getBroadcastEnabledDataset(rightTableSet, rightTable.getRightTable(), plan));
                plan.add(String.format("Right table %s , Join Type : %s ", rightTable.getRightTable().getName(), rightTable.getJoinType()));
            }
            joinedSet.explain(true);
        }
        joinedSet = this.select((PipelineNode)this.node, entity -> String.format("%s.%s AS %s", entity.getReferencedEntity().getOwnerNode().getId(), entity.getInputName(), entity.getOutputName()), joinedSet, i -> ((JoinNode)this.node).getInputEntitiesUsed(this.requestContext.getProcessingContext().isValidationMode()));
        joinedSet = this.appendDerivations(this.node, joinedSet, plan);
        joinedSet = this.select(this.node, joinedSet);
        joinedSet = this.appendPostActions(this.node, joinedSet, plan);
        this.sparkTranslatorState.addPlan(this.node, plan);
        final DataFrameObject joinedDFO = new DataFrameObject(joinedSet, String.join("\n", plan));
        this.sparkTranslatorState.addDataFrame(this.node, joinedDFO);
    }
    
    protected Translator start(final String id) {
        return null;
    }
    
    protected void end() {
    }
}
