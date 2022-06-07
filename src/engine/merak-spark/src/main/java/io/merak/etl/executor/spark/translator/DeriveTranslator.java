package io.merak.etl.executor.spark.translator;

import io.merak.etl.translator.*;
import io.merak.etl.executor.spark.translator.dto.*;
import io.merak.etl.pipeline.dto.*;
import com.google.common.collect.*;

import java.util.*;
import org.apache.spark.sql.*;

public class DeriveTranslator extends SparkTranslator<DeriveNode>
{
    public DeriveTranslator(final DeriveNode node, final TranslatorContext translatorContext) {
        super((PipelineNode)node, translatorContext);
    }
    
    @Override
    protected void generateDataFrame() {
        final List<String> plan = (List<String>)Lists.newArrayList();
        plan.add(String.format("Node : %s", ((DeriveNode)this.node).getId()));
        final PipelineNode parent = this.getParentNode((PipelineNode)this.node);
        final DataFrameObject incomingDFO = this.sparkTranslatorState.getDataFrame(parent);
        Dataset<Row> deriveSet = incomingDFO.getDataset();
        deriveSet = this.appendDerivations(this.node, deriveSet, plan);
        deriveSet = this.select(this.node, deriveSet);
        this.sparkTranslatorState.addPlan(this.node, plan);
        final DataFrameObject deriveDFO = new DataFrameObject(deriveSet, String.join("\n", plan));
        this.sparkTranslatorState.addDataFrame(this.node, deriveDFO);
    }
}
