package io.merak.etl.executor.spark.translator;

import io.merak.etl.translator.*;
import io.merak.etl.executor.spark.translator.dto.*;
import io.merak.etl.executor.spark.utils.*;

import java.util.*;

import org.apache.spark.sql.*;

import com.google.common.collect.*;
import java.util.stream.*;
import io.merak.etl.pipeline.dto.*;

public class InNotInTranslator extends SparkTranslator<InNotInNode>
{
    public InNotInTranslator(final InNotInNode node, final TranslatorContext translatorContext) {
        super((PipelineNode)node, translatorContext);
    }
    
    private Dataset<Row> applyFilter(Dataset<Row> outerSet, final List<String> plan) {
        final String predicate = ((InNotInNode)this.node).getPredicate();
        final String filterType = ((InNotInNode)this.node).getType();
        plan.add(String.format("Filter Type : %s", filterType));
        final Column outerColumn = SparkUtils.getColumnFromExp(this.spark, String.format("%s.%s", ((InNotInNode)this.node).getOuterTable().getId(), ((InNotInNode)this.node).getOuterEntity().getInputName()));
        plan.add(String.format("Outer Table column : %s ", String.format("%s.%s", ((InNotInNode)this.node).getOuterTable().getId(), ((InNotInNode)this.node).getInnerEntity().getOutputName((PipelineNode)this.node))));
        final PipelineNode innerTable = ((InNotInNode)this.node).getInnerTable();
        final DataFrameObject innerDFO = this.sparkTranslatorState.getDataFrame(innerTable);
        Dataset<Row> innerTableSet = (Dataset<Row>)innerDFO.getDataset().as(((InNotInNode)this.node).getInnerTable().getId());
        final String innerCol = String.format("%s as %s ", ((InNotInNode)this.node).getInnerEntity().getInputName(), "ZIW_" + ((InNotInNode)this.node).getInnerEntity().getInputName());
        plan.add(String.format("Inner Table Column : %s", innerCol));
        final Column innerColumn = SparkUtils.getColumnFromExp(this.spark, innerCol);
        innerTableSet = (Dataset<Row>)innerTableSet.select(new Column[] { innerColumn });
        if (filterType.equals("NOT IN")) {
            outerSet = (Dataset<Row>)outerSet.filter(functions.not(outerColumn.isin(((List)innerTableSet.collectAsList().stream().map(row -> row.get(0)).collect(Collectors.toList())).toArray())));
        }
        else {
            outerSet = (Dataset<Row>)outerSet.filter(outerColumn.isin(((List)innerTableSet.collectAsList().stream().map(row -> row.get(0)).collect(Collectors.toList())).toArray()));
        }
        return outerSet;
    }
    
    @Override
    protected void generateDataFrame() {
        final List<String> plan = (List<String>)Lists.newArrayList();
        final PipelineNode parent = ((InNotInNode)this.node).getOuterTable();
        plan.add(String.format("Outer Table Id : %s", parent.getId()));
        final DataFrameObject outerDFO = this.sparkTranslatorState.getDataFrame(parent);
        Dataset<Row> outerTableSet = (Dataset<Row>)outerDFO.getDataset().as(parent.getId());
        outerTableSet = this.applyFilter(outerTableSet, plan);
        outerTableSet = this.select((PipelineNode)this.node, entity -> String.format("%s as %s", entity.getInputName(), entity.getOutputName()), outerTableSet, i -> ((InNotInNode)this.node).getInputEntitiesUsedByPortId(this.requestContext.getProcessingContext().isValidationMode(), ((InNotInNode)this.node).getOuterPortId()).stream());
        this.sparkTranslatorState.addPlan(this.node, plan);
        final DataFrameObject inNotInDFO = new DataFrameObject(outerTableSet, String.join("\n", plan));
        this.sparkTranslatorState.addDataFrame(this.node, inNotInDFO);
    }
}
