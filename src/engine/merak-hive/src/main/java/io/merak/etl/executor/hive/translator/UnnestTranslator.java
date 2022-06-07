package io.merak.etl.executor.hive.translator;

import java.util.*;
import com.google.common.base.*;

import io.merak.etl.pipeline.dto.*;
import io.merak.etl.sdk.task.*;
import io.merak.etl.translator.*;

import java.util.stream.*;

public class UnnestTranslator extends SqlTranslator
{
    private final UnnestNode node;
    private final Map<String, UnnestFunction> unnestFunctionToHiveUdtf;
    
    public UnnestTranslator(final UnnestNode node, final TranslatorContext translatorContext) {
        super(translatorContext);
        this.unnestFunctionToHiveUdtf = new HashMap<String, UnnestFunction>() {
            {
                this.put("unnest_array", new UnnestFunction("explode", 1));
                this.put("unnest_array_with_position", new UnnestFunction("posexplode", 2));
                this.put("unnest_map", new UnnestFunction("explode", 2));
            }
        };
        this.node = node;
    }
    
    private SqlTranslator unnest() {
        for (int i = 0; i < this.node.getPropertyDerives().size(); ++i) {
            final Derivation<UnnestNode.CustomAttributes> derive = (Derivation<UnnestNode.CustomAttributes>)this.node.getPropertyDerives().get(i);
            String expression = ((UnnestNode.CustomAttributes)derive.getCustomAttributes()).getExpression();
            if (UnnestNode.MODE_TYPE.SIMPLE.equals((Object)((UnnestNode.CustomAttributes)derive.getCustomAttributes()).getMode())) {
                final UnnestFunction function = this.unnestFunctionToHiveUdtf.get(((UnnestNode.CustomAttributes)derive.getCustomAttributes()).getFunction());
                Preconditions.checkNotNull((Object)function, "Invalid function %s", new Object[] { ((UnnestNode.CustomAttributes)derive.getCustomAttributes()).getFunction() });
                Preconditions.checkState(function.outputs == derive.getDerivedFields().size(), "Invalid number of output columns %s, expected: %s", new Object[] { derive.getDerivedFields().size(), function.outputs });
                expression = String.format("%s(%s)", function.hiveFunction, ((UnnestNode.CustomAttributes)derive.getCustomAttributes()).getExpression());
            }
            final String deriveNames = (String)derive.getDerivedFields().stream().map(c -> c.getOutputName((PipelineNode)this.node)).collect(Collectors.joining(", "));
            String viewTypeExpression = UnnestNode.VIEW_TYPE.OUTER.toString();
            final UnnestNode.VIEW_TYPE viewType = ((UnnestNode.CustomAttributes)derive.getCustomAttributes()).getViewType();
            if (viewType != null) {
                viewTypeExpression = ((UnnestNode.CustomAttributes)derive.getCustomAttributes()).getViewType().toString();
            }
            this.builder.append(String.format(" LATERAL VIEW %s %s %s_t%d AS %s", viewTypeExpression, expression, this.node.getId(), i, deriveNames));
        }
        return this;
    }
    
    public TaskNode translate() {
        String parent = this.getParent((PipelineNode)this.node);
        parent = this.appendDerivations((PipelineNode)this.node, parent);
        this.start(this.node.getId()).select((PipelineNode)this.node).from(parent);
        this.unnest().end();
        return null;
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
