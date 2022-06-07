package io.merak.etl.executor.hive.translator.previewsql;

import io.merak.etl.executor.hive.task.*;
import io.merak.etl.pipeline.dto.*;
import io.merak.etl.sdk.engine.*;
import io.merak.etl.sdk.task.*;
import io.merak.etl.translator.*;

import java.util.*;
import com.google.common.base.*;

import java.util.stream.*;

public class UnnestTranslator extends NodeSqlTranslator
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
    
    private NodeSqlTranslator unnest() {
        for (int i = 0; i < this.node.getPropertyDerives().size(); ++i) {
            final Derivation<UnnestNode.CustomAttributes> derive = (Derivation<UnnestNode.CustomAttributes>)this.node.getPropertyDerives().get(i);
            String expression = ((UnnestNode.CustomAttributes)derive.getCustomAttributes()).getOriginalExpression();
            String viewTypeExpression = UnnestNode.VIEW_TYPE.OUTER.toString();
            if (UnnestNode.MODE_TYPE.SIMPLE.equals((Object)((UnnestNode.CustomAttributes)derive.getCustomAttributes()).getMode())) {
                final UnnestFunction function = this.unnestFunctionToHiveUdtf.get(((UnnestNode.CustomAttributes)derive.getCustomAttributes()).getFunction());
                Preconditions.checkNotNull((Object)function, "Invalid function %s", new Object[] { ((UnnestNode.CustomAttributes)derive.getCustomAttributes()).getFunction() });
                Preconditions.checkState(function.outputs == derive.getDerivedFields().size(), "Invalid number of output columns %s, expected: %s", new Object[] { derive.getDerivedFields().size(), function.outputs });
                String functionName = function.hiveFunction;
                if (BatchEngine.SPARK.equals((Object)this.requestContext.getBatchEngine())) {
                    final UnnestNode.VIEW_TYPE viewType = ((UnnestNode.CustomAttributes)derive.getCustomAttributes()).getViewType();
                    if (viewType != null && !viewType.toString().isEmpty()) {
                        functionName = String.format("%s_%s", function.hiveFunction, viewType.toString().toLowerCase());
                    }
                }
                expression = String.format("%s(%s)", functionName, ((UnnestNode.CustomAttributes)derive.getCustomAttributes()).getOriginalExpression());
            }
            final String deriveNames = (String)derive.getDerivedFields().stream().map(entity -> String.format("`%s`", entity.getGivenName())).collect(Collectors.joining(", "));
            final UnnestNode.VIEW_TYPE viewType2 = ((UnnestNode.CustomAttributes)derive.getCustomAttributes()).getViewType();
            if (viewType2 != null) {
                viewTypeExpression = ((UnnestNode.CustomAttributes)derive.getCustomAttributes()).getViewType().toString();
            }
            this.builder.append(String.format(" LATERAL VIEW %s %s %s_t%d AS %s", viewTypeExpression, expression, this.node.getId(), i, deriveNames));
        }
        return this;
    }
    
    public TaskNode translate() {
        String parent = this.getParent((PipelineNode)this.node);
        parent = this.appendDerivations((PipelineNode)this.node, parent);
        this.start(this.node.getName());
        String oldName;
        this.select((PipelineNode)this.node, entity -> {
            oldName = entity.getGivenName();
            if (entity.getReferencedEntity() != null && entity.getReferencedEntity().getGivenName() != null) {
                oldName = entity.getReferencedEntity().getGivenName();
            }
            return String.format("`%s` AS `%s`", oldName, entity.getGivenName());
        }, () -> this.node.getOutputEntities().stream().filter(entity -> !entity.getReferenceType().equals((Object)Entity.Mapping.REFERENCE_TYPE.PROPERTY))).from(parent);
        this.unnest();
        this.end();
        return (TaskNode)new QueryTaskNode(this.builder.toString());
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
