package io.merak.etl.executor.spark.translator;

import io.merak.etl.translator.*;
import io.merak.etl.executor.spark.translator.dto.*;

import java.util.*;
import org.apache.spark.sql.*;
import com.google.common.collect.*;
import com.google.common.base.*;
import java.util.stream.*;

import io.merak.etl.pipeline.dto.*;

public class UnnestTranslator extends SparkTranslator<UnnestNode>
{
    private final Map<String, UnnestFunction> unnestFunctionToHiveUdtf;
    
    public UnnestTranslator(final UnnestNode node, final TranslatorContext translatorContext) {
        super((PipelineNode)node, translatorContext);
        this.unnestFunctionToHiveUdtf = new HashMap<String, UnnestFunction>() {
            {
                this.put("unnest_array", new UnnestFunction("explode", 1));
                this.put("unnest_array_with_position", new UnnestFunction("posexplode", 2));
                this.put("unnest_map", new UnnestFunction("explode", 2));
            }
        };
    }
    
    private Dataset<Row> unnest(final List<String> plan, Dataset<Row> unnestSet) {
        final List<String> splitOperations = (List<String>)Lists.newArrayList();
        for (int i = 0; i < ((UnnestNode)this.node).getPropertyDerives().size(); ++i) {
            final Derivation<UnnestNode.CustomAttributes> derive = (Derivation<UnnestNode.CustomAttributes>)((UnnestNode)this.node).getPropertyDerives().get(i);
            String expression = String.format("%s", ((UnnestNode.CustomAttributes)derive.getCustomAttributes()).getExpression());
            if (UnnestNode.MODE_TYPE.SIMPLE.equals((Object)((UnnestNode.CustomAttributes)derive.getCustomAttributes()).getMode())) {
                final UnnestFunction function = this.unnestFunctionToHiveUdtf.get(((UnnestNode.CustomAttributes)derive.getCustomAttributes()).getFunction());
                Preconditions.checkNotNull((Object)function, "Invalid function %s", new Object[] { ((UnnestNode.CustomAttributes)derive.getCustomAttributes()).getFunction() });
                Preconditions.checkState(function.outputs == derive.getDerivedFields().size(), "Invalid number of output columns %s, expected: %s", new Object[] { derive.getDerivedFields().size(), function.outputs });
                final UnnestNode.VIEW_TYPE viewType = ((UnnestNode.CustomAttributes)derive.getCustomAttributes()).getViewType();
                String functionName = function.hiveFunction;
                if (viewType != null) {
                    functionName = (viewType.toString().isEmpty() ? function.hiveFunction : String.format("%s_%s", function.hiveFunction, viewType.toString().toLowerCase()));
                }
                expression = String.format(" %s(%s) ", functionName, ((UnnestNode.CustomAttributes)derive.getCustomAttributes()).getExpression());
            }
            final String deriveNames = (String)derive.getDerivedFields().stream().map(c -> c.getOutputName((PipelineNode)this.node)).collect(Collectors.joining(", "));
            splitOperations.add(String.format("{Expression : %s, Derived Columns : %s}", expression, deriveNames));
            unnestSet = (Dataset<Row>)unnestSet.selectExpr(new String[] { "*", String.format("%s as (%s)", expression, deriveNames) });
        }
        plan.add(String.format("Split Functions : %s", String.join(",", splitOperations)));
        return unnestSet;
    }
    
    @Override
    protected void generateDataFrame() {
        final List<String> plan = (List<String>)Lists.newArrayList();
        plan.add(String.format("Node : %s", ((UnnestNode)this.node).getId()));
        final PipelineNode parent = this.getParentNode((PipelineNode)this.node);
        final DataFrameObject incomingDFO = this.sparkTranslatorState.getDataFrame(parent);
        Dataset<Row> unnestSet = incomingDFO.getDataset();
        unnestSet = this.appendDerivations(this.node, unnestSet, plan);
        unnestSet = this.unnest(plan, unnestSet);
        unnestSet = this.select(this.node, unnestSet);
        this.sparkTranslatorState.addPlan(this.node, plan);
        final DataFrameObject unnestDFO = new DataFrameObject(unnestSet, String.join("\n", plan));
        this.sparkTranslatorState.addDataFrame(this.node, unnestDFO);
    }
    
    private class UnnestFunction
    {
        final String hiveFunction;
        final int outputs;
        
        public UnnestFunction(final String hiveFunction, final int outputs) {
            this.hiveFunction = hiveFunction;
            this.outputs = outputs;
        }
    }
}
