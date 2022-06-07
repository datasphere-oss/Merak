package io.merak.etl.executor.spark.translator;

import io.merak.etl.translator.*;
import io.merak.etl.executor.spark.translator.dto.*;

import com.google.common.collect.*;

import java.util.*;
import org.apache.spark.sql.*;
import java.util.stream.*;
import io.merak.etl.pipeline.dto.*;

public class FuzzyMatchingTranslator extends SparkTranslator<FuzzyMatchingNode>
{
    public FuzzyMatchingTranslator(final FuzzyMatchingNode node, final TranslatorContext translatorContext) {
        super((PipelineNode)node, translatorContext);
    }
    
    @Override
    protected void generateDataFrame() {
        final List<String> plan = (List<String>)Lists.newArrayList();
        plan.add(String.format("Node : %s", ((FuzzyMatchingNode)this.node).getId()));
        final PipelineNode lookupTable = ((FuzzyMatchingNode)this.node).getLookupTable();
        final DataFrameObject lookupTableDFO = this.sparkTranslatorState.getDataFrame(lookupTable);
        final Dataset<Row> lookupTableSet = lookupTableDFO.getDataset();
        final PipelineNode inputTable = ((FuzzyMatchingNode)this.node).getInputTable();
        final DataFrameObject inputTableDFO = this.sparkTranslatorState.getDataFrame(inputTable);
        final Dataset<Row> inputTableSet = inputTableDFO.getDataset();
        Dataset<Row> mergedDataset = null;
        for (final FuzzyMatchingNode.MatchColumns matchColumns : ((FuzzyMatchingNode)this.node).getMatchColumns()) {
            if (matchColumns.getMatchtype().equals("EXACT")) {
                final Column joinExpression = this.getJoinExpression(inputTableSet, lookupTableSet, ((FuzzyMatchingNode)this.node).getMatchColumns());
                System.out.println("joinexpression" + joinExpression);
                mergedDataset = (Dataset<Row>)inputTableSet.join((Dataset)lookupTableSet, joinExpression, "inner");
            }
            else {
                mergedDataset = (Dataset<Row>)inputTableSet.join((Dataset)lookupTableSet);
            }
        }
        plan.add("LookupTable : " + lookupTable.getName());
        plan.add("InputTable : " + inputTable.getName());
        for (final FuzzyMatchingNode.MatchColumns fuzzyMatchColumns : ((FuzzyMatchingNode)this.node).getMatchColumns()) {
            if (fuzzyMatchColumns.getMatchtype().equals("FUZZY")) {
                if (fuzzyMatchColumns.getMatching_algorithm().equals("LEVENSHTEIN")) {
                    mergedDataset = this.computeFuzzyScore(mergedDataset, ((FuzzyMatchingNode)this.node).getMatchColumns());
                }
                else {
                    if (!fuzzyMatchColumns.getMatching_algorithm().equals("SOUNDEX")) {
                        continue;
                    }
                    mergedDataset = this.soundexAlgo(mergedDataset, inputTableSet, lookupTableSet, ((FuzzyMatchingNode)this.node).getMatchColumns());
                }
            }
        }
        mergedDataset = this.select((PipelineNode)this.node, entity -> String.format("%s AS %s", entity.getInputName(), entity.getOutputName()), mergedDataset, i -> ((FuzzyMatchingNode)this.node).getInputEntitiesUsed(this.requestContext.getProcessingContext().isValidationMode()));
        mergedDataset = this.select(this.node, mergedDataset);
        this.sparkTranslatorState.addPlan(this.node, plan);
        final DataFrameObject fuzzyMatchDTO = new DataFrameObject(mergedDataset, String.join("\n", plan));
        this.sparkTranslatorState.addDataFrame(this.node, fuzzyMatchDTO);
    }
    
    private Column getJoinExpression(final Dataset inputTableSet, final Dataset lookupTableSet, final List<FuzzyMatchingNode.MatchColumns> Matchcolumns) {
        Column column = null;
        for (final FuzzyMatchingNode.MatchColumns Matchcolumn : Matchcolumns) {
            final String inputColumn = Matchcolumn.getInputColumn().getInputName();
            final String lookupColumn = Matchcolumn.getLookupColumn().getInputName();
            if (column == null) {
                column = inputTableSet.col(inputColumn).equalTo((Object)lookupTableSet.col(lookupColumn));
            }
            else {
                column.and(inputTableSet.col(inputColumn).equalTo((Object)lookupTableSet.col(lookupColumn)));
            }
        }
        return column;
    }
    
    private Dataset<Row> computeFuzzyScore(final Dataset<Row> mergedDataset, final List<FuzzyMatchingNode.MatchColumns> fuzzyMatchColumns) {
        for (final FuzzyMatchingNode.MatchColumns fuzzyMatchColumn : fuzzyMatchColumns) {
            final String inputColumnName = fuzzyMatchColumn.getInputColumn().getInputName();
            final Column inputColumn = mergedDataset.col(inputColumnName);
            final String lookupColumnName = fuzzyMatchColumn.getLookupColumn().getInputName();
            final Column lookupColumn = mergedDataset.col(lookupColumnName);
            mergedDataset.withColumn("Matching_Score", functions.levenshtein(inputColumn, lookupColumn));
        }
        return mergedDataset;
    }
    
    private Dataset<Row> soundexAlgo(final Dataset<Row> mergedDataset, final Dataset<Row> inputDataset, final Dataset<Row> lookupDataset, final List<FuzzyMatchingNode.MatchColumns> fuzzyMatchColumns) {
        if (((FuzzyMatchingNode)this.node).getMatchColumns() == null || ((FuzzyMatchingNode)this.node).getMatchColumns().isEmpty()) {
            return mergedDataset;
        }
        for (final FuzzyMatchingNode.MatchColumns fuzzyMatchColumn : fuzzyMatchColumns) {
            final String inputColumnName = fuzzyMatchColumn.getInputColumn().getInputName();
            final Column inputColumn = inputDataset.col(inputColumnName);
            final String lookupColumnName = fuzzyMatchColumn.getLookupColumn().getInputName();
            final Column lookupColumn = lookupDataset.col(lookupColumnName);
            mergedDataset.withColumn("Soundex_Equality", functions.when(functions.soundex(inputColumn).equalTo((Object)functions.soundex(lookupColumn)), (Object)true).otherwise((Object)false));
        }
        return mergedDataset;
    }
}
