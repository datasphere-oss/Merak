package io.merak.etl.executor.hive.translator.previewsql;

import java.util.function.*;
import java.util.stream.*;

import io.merak.etl.pipeline.dto.*;
import io.merak.etl.sdk.dag.api.*;
import io.merak.etl.translator.*;

import java.util.*;

public abstract class NodeSqlTranslator extends Translator
{
    protected final StringBuilder builder;
    
    public NodeSqlTranslator(final TranslatorContext translatorContext) {
        super(translatorContext);
        this.builder = (StringBuilder)translatorContext.getTranslatorState().get();
    }
    
    protected NodeSqlTranslator start(final String name) {
        this.builder.append(String.format("`%s`", name)).append(" AS (");
        return this;
    }
    
    protected void end() {
        this.builder.append("),\n");
    }
    
    protected void end(final StringBuilder builder) {
        builder.append("),\n");
    }
    
    protected NodeSqlTranslator select(final PipelineNode node) {
        return this.select(node, this::defaultSelectFormatter);
    }
    
    protected NodeSqlTranslator select(final PipelineNode node, final Function<Entity, String> formatter, final String keyword) {
        return this.select(node, formatter, keyword, () -> node.getOutputEntities().stream());
    }
    
    protected NodeSqlTranslator select(final PipelineNode node, final Function<Entity, String> formatter) {
        return this.select(node, formatter, "SELECT ", () -> node.getOutputEntities().stream());
    }
    
    protected NodeSqlTranslator select(final PipelineNode node, final Function<Entity, String> formatter, final Supplier<Stream<Entity>> entitiesUsedFormatter) {
        return this.select(node, formatter, "SELECT ", entitiesUsedFormatter);
    }
    
    protected NodeSqlTranslator select(final PipelineNode node, final Function<Entity, String> formatter, final String keyword, final Supplier<Stream<Entity>> entitiesUsedFormatter) {
        this.builder.append(keyword);
        String columns = entitiesUsedFormatter.get().map((Function<? super Entity, ?>)formatter).filter(Objects::nonNull).collect((Collector<? super Object, ?, String>)Collectors.joining(", "));
        if (columns.isEmpty()) {
            columns = "1";
        }
        this.builder.append(columns);
        return this;
    }
    
    protected String defaultSelectFormatter(final Entity entity) {
        String columnExpr = String.format("`%s`", entity.getGivenName());
        if (entity.getReferenceType() != null) {
            switch (entity.getReferenceType()) {
                case INPUT: {
                    if (entity.getReferencedEntity() != null) {
                        columnExpr = String.format("`%s` AS `%s`", entity.getReferencedEntity().getGivenName(), entity.getGivenName());
                        break;
                    }
                    break;
                }
                case DERIVATION: {
                    columnExpr = String.format("`%s` AS `%s`", entity.getGivenName(), entity.getGivenName());
                    break;
                }
                case PROPERTY: {
                    columnExpr = String.format("%s AS `%s`", ((Derivation)entity.getReferencedEntity()).getOriginalExpression(), entity.getGivenName());
                    break;
                }
            }
        }
        return columnExpr;
    }
    
    protected boolean canUseStarInSelect(final PipelineNode node) {
        return node.getOutputEntitiesUsed(true).count() != 0L && this.hasNoRenames(node);
    }
    
    public boolean hasNoRenames(final PipelineNode node) {
        return node.getOutputEntities().stream().allMatch(c -> c.getReferencedEntity() != null && c.getGivenName().equalsIgnoreCase(c.getReferencedEntity().getGivenName()));
    }
    
    protected NodeSqlTranslator from(final String from) {
        this.builder.append(" FROM ").append(String.format("`%s`", from));
        return this;
    }
    
    public String appendTemporaryDerivations(final PipelineNode node, String parentNodeName) {
        int i = 0;
        for (final Derivation derive : node.getDerivations()) {
            if (derive.getDerivationOutputType().equals((Object)Derivation.DERIVATION_OUTPUT_TYPE.OUTPUT)) {
                continue;
            }
            parentNodeName = this.appendSingleDerivation(node, parentNodeName, derive, i++, false);
        }
        return parentNodeName;
    }
    
    public String appendDerivations(final PipelineNode node, String parentNodeName) {
        int i = 0;
        for (final Derivation derive : node.getDerivations()) {
            parentNodeName = this.appendSingleDerivation(node, parentNodeName, derive, i++, true);
        }
        return parentNodeName;
    }
    
    private String appendSingleDerivation(final PipelineNode node, String parentNodeName, final Derivation derive, int i, final boolean hasOutputDerives) {
        final String EXPLODED_ARRAY = "iw_exploded";
        String newNodeName = this.getNewNodeName(node, i++, hasOutputDerives);
        if (derive.getDerivationOutputCount() == 1) {
            this.builder.append(String.format("`%s` AS (SELECT %s.*, %s AS `%s` FROM `%s`),\n", newNodeName, parentNodeName, derive.getOriginalExpression(), derive.getDerivedFields().get(0).getGivenName(), parentNodeName));
        }
        else {
            this.builder.append(String.format("`%s` AS (SELECT `%s`.*, %s AS `%s` FROM `%s`),\n", newNodeName, parentNodeName, derive.getOriginalExpression(), EXPLODED_ARRAY, parentNodeName));
            parentNodeName = newNodeName;
            newNodeName = this.getNewNodeName(node, i++, hasOutputDerives);
            this.builder.append(String.format("`%s` AS (SELECT `%s`.*", newNodeName, parentNodeName));
            for (int i2 = 0; i2 < derive.getDerivationOutputCount(); ++i2) {
                final Entity col = derive.getDerivedFields().get(i2);
                this.builder.append(String.format(", %s[%d] AS `%s`", EXPLODED_ARRAY, i2, col.getGivenName()));
            }
            this.builder.append(String.format(" FROM `%s`),\n", parentNodeName));
        }
        parentNodeName = newNodeName;
        return parentNodeName;
    }
    
    protected String getNewNodeName(final PipelineNode node, final int index, final boolean hasOutputDerives) {
        return String.format("%s_%s%d", node.getName(), hasOutputDerives ? "o" : "t", index);
    }
    
    public String getParent(final PipelineNode node) {
        return this.translatorContext.getPipelineDag().getIncomingNodes((DagNode)node).get(0).getName();
    }
    
    protected void appendPostActions(final PipelineNode node) {
        if (node.getPostProcessingConfigs() != null && node.getPostProcessingConfigs().isDistributionEnabled()) {
            if (node.getPostProcessingConfigs().getDistributionEntities().size() > 0) {
                final String distributeColumns = (String)node.getPostProcessingConfigs().getDistributionEntities().stream().map(Entity::getGivenName).collect(Collectors.joining("`,`"));
                this.builder.append(String.format(" DISTRIBUTE BY `%s` ", distributeColumns));
            }
            if (node.getPostProcessingConfigs().isSortingEnabled() && node.getPostProcessingConfigs().getSortEntities().size() > 0) {
                final String sortColumns = (String)node.getPostProcessingConfigs().getSortEntities().stream().map(Entity::getGivenName).collect(Collectors.joining("`,`"));
                this.builder.append(String.format(" SORT BY `%s` ", sortColumns));
            }
        }
    }
}
