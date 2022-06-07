package io.merak.etl.executor.spark.translator;

import io.merak.etl.translator.*;
import io.merak.etl.executor.spark.translator.dto.*;

import org.apache.spark.sql.*;
import io.merak.etl.pipeline.dto.*;
import java.util.function.*;
import java.util.stream.*;
import com.google.common.collect.*;

import java.util.*;

public class UnionTranslator extends SparkTranslator<UnionNode>
{
    public UnionTranslator(final UnionNode node, final TranslatorContext translatorContext) {
        super((PipelineNode)node, translatorContext);
    }
    
    @Override
    protected Dataset<Row> select(final PipelineNode unionNode, final Dataset<Row> dataframe) {
        final Entity inputEntity;
        return this.select((PipelineNode)this.node, c -> {
            inputEntity = ((UnionNode)this.node).getUnionNodeInputEntity(unionNode.getId(), c);
            return String.format("%s AS %s", inputEntity.getInputName(), c.getInputName());
        }, dataframe, (UnionNode)this.node::getOutputEntitiesUsed);
    }
    
    @Override
    protected boolean canUseStarInSelect(final Function<Boolean, Stream<Entity>> entitiesUsedFormatter) {
        return false;
    }
    
    @Override
    protected void generateDataFrame() {
        final List<String> plan = (List<String>)Lists.newArrayList();
        final PipelineNode primaryTableNode = ((UnionNode)this.node).getUnionNodes().get(0);
        final DataFrameObject primaryTableDFO = this.sparkTranslatorState.getDataFrame(primaryTableNode);
        Dataset<Row> primaryDataSet = primaryTableDFO.getDataset();
        primaryDataSet = this.select(primaryTableNode, primaryDataSet);
        plan.add(String.format("Node : %s", ((UnionNode)this.node).getId()));
        plan.add("Primary Union Table : " + primaryTableNode.getName());
        Dataset<Row> unionSet = primaryDataSet;
        for (int i = 1; i < ((UnionNode)this.node).getUnionNodes().size(); ++i) {
            final PipelineNode unionNode = ((UnionNode)this.node).getUnionNodes().get(i);
            final DataFrameObject unionTableDFO = this.sparkTranslatorState.getDataFrame(unionNode);
            Dataset<Row> unionDataSet = unionTableDFO.getDataset();
            unionDataSet = this.select(((UnionNode)this.node).getUnionNodes().get(i), unionDataSet);
            if (((UnionNode)this.node).getUnionType().name().equals("ALL")) {
                unionSet = (Dataset<Row>)unionSet.unionAll((Dataset)unionDataSet);
                plan.add(String.format("Union Table-%s : %s, Union Type : ALL", i, unionNode.getName()));
            }
            else {
                unionSet = (Dataset<Row>)unionSet.unionAll((Dataset)unionDataSet).dropDuplicates();
                plan.add(String.format("Union Table-%s : %s, Union Type : DISTINCT ", i, unionNode.getName()));
            }
        }
        this.sparkTranslatorState.addPlan(this.node, plan);
        final DataFrameObject unionDFO = new DataFrameObject(unionSet, String.join("\n", plan));
        this.sparkTranslatorState.addDataFrame(this.node, unionDFO);
    }
}
