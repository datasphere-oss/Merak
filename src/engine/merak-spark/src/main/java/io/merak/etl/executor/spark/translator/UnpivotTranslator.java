package io.merak.etl.executor.spark.translator;

import io.merak.etl.translator.*;
import io.merak.etl.executor.spark.translator.dto.*;

import org.apache.spark.sql.*;
import java.util.*;
import java.util.stream.*;
import com.google.common.collect.*;

import io.merak.etl.pipeline.dto.*;

public class UnpivotTranslator extends SparkTranslator<UnpivotNode>
{
    public UnpivotTranslator(final UnpivotNode node, final TranslatorContext translatorContext) {
        super((PipelineNode)node, translatorContext);
    }
    
    private Dataset<Row> unpivot(Dataset<Row> unpivotSet, final List<String> plan) {
        final String deriveNames = (String)((UnpivotNode)this.node).getProperties().getDerives().get(0).getDerivedFields().stream().map(c -> c.getOutputName((PipelineNode)this.node)).collect(Collectors.joining(", "));
        unpivotSet = (Dataset<Row>)unpivotSet.selectExpr(new String[] { "*", String.format("explode(map(%s)) as (%s)", this.getMapExpression(), deriveNames) });
        return unpivotSet;
    }
    
    private String getMapExpression() {
        final List<String> mapColumns = (List<String>)((UnpivotNode)this.node).getUnpivotEntities().stream().map(i -> String.format("'%s',%s", i.getGivenName(), i.getOutputName((PipelineNode)this.node))).collect(Collectors.toList());
        final String mapExpression = String.join(",", mapColumns);
        return mapExpression;
    }
    
    @Override
    protected void generateDataFrame() {
        final List<String> plan = (List<String>)Lists.newArrayList();
        plan.add(String.format("Node : %s", ((UnpivotNode)this.node).getId()));
        final PipelineNode parent = this.getParentNode((PipelineNode)this.node);
        final DataFrameObject incomingDFO = this.sparkTranslatorState.getDataFrame(parent);
        Dataset<Row> unpivotSet = incomingDFO.getDataset();
        unpivotSet = this.appendDerivations(this.node, unpivotSet, plan);
        unpivotSet = this.unpivot(unpivotSet, plan);
        unpivotSet = this.select(this.node, unpivotSet);
        this.sparkTranslatorState.addPlan(this.node, plan);
        final DataFrameObject unpivotDFO = new DataFrameObject(unpivotSet, String.join("\n", plan));
        this.sparkTranslatorState.addDataFrame(this.node, unpivotDFO);
    }
}
