package io.merak.etl.executor.spark.translator;

import io.merak.etl.translator.*;
import io.merak.etl.executor.spark.translator.dto.*;

import com.google.common.collect.*;

import java.util.*;
import org.apache.spark.sql.*;
import java.util.function.*;
import java.util.stream.*;
import io.merak.etl.pipeline.dto.*;

public class SourceTranslator extends SparkTranslator<SourceNode>
{
    private SourceTranslatorUtils sourceTranslatorUtils;
    
    public SourceTranslator(final SourceNode node, final TranslatorContext translatorContext) {
        super((PipelineNode)node, translatorContext);
        this.sourceTranslatorUtils = new SourceTranslatorUtils(node, translatorContext.getRequestContext());
    }
    
    private String getTableQuery(final boolean handleDeletedRecords) {
        final StringBuilder sourceQuery = new StringBuilder();
        if (!handleDeletedRecords) {
            if (!((SourceNode)this.node).isView()) {
                sourceQuery.append(String.format(" `%s`.`%s` ", ((SourceNode)this.node).getSchemaName(), ((SourceNode)this.node).getTableName()));
            }
            else {
                final String viewQuery = this.sourceTranslatorUtils.getSourceViewQuery();
                sourceQuery.append(String.format(" ( %s ) as SOURCE_view_%s", viewQuery, ((SourceNode)this.node).getId()));
            }
        }
        else {
            sourceQuery.append(String.format(" `%s`.`%s` ", ((SourceNode)this.node).getSchemaName(), this.sourceTranslatorUtils.getHistoryViewName(((SourceNode)this.node).getTableName())));
        }
        return sourceQuery.toString();
    }
    
    private String getFilterQuery(final boolean handleDeletedRecords) {
        final StringBuilder filterQuery = new StringBuilder();
        if (!handleDeletedRecords) {
            if (!((SourceNode)this.node).isView() && ((SourceNode)this.node).loadIncrementally()) {
                filterQuery.append(this.sourceTranslatorUtils.getIncrementalFilterCondition());
            }
        }
        else {
            if (((SourceNode)this.node).loadIncrementally()) {
                filterQuery.append(this.sourceTranslatorUtils.getIncrementalFilterCondition());
                filterQuery.append(" AND ");
            }
            else {
                filterQuery.append(" WHERE ");
            }
            filterQuery.append(" `ZIW_IS_DELETED`='true' ");
        }
        return filterQuery.toString();
    }
    
    private String getQuery() {
        final StringBuilder builder = new StringBuilder(String.format("SELECT * FROM %s %s", this.getTableQuery(false), this.getFilterQuery(false)));
        if (((SourceNode)this.node).handleDeleteRecords() && !((SourceNode)this.node).isInjected()) {
            builder.append(" UNION ");
            builder.append(String.format("SELECT * FROM %s %s", this.getTableQuery(true), this.getFilterQuery(true)));
        }
        return builder.toString();
    }
    
    @Override
    protected void generateDataFrame() {
        final List<String> plan = (List<String>)Lists.newArrayList();
        plan.add(String.format("Node : %s ", ((SourceNode)this.node).getId()));
        final String sourceQuery = this.getQuery();
        this.LOGGER.info("Source Query = {}", (Object)sourceQuery);
        Dataset<Row> source = (Dataset<Row>)this.spark.sql(sourceQuery);
        plan.add(String.format("Source query : %s", sourceQuery));
        this.sparkTranslatorState.addPlan(this.node, plan);
        this.LOGGER.debug("Source schema before is :{}", (Object)source.schema().treeString());
        source = this.select(this.node, this::sourceSelectFormatter, source, c -> ((SourceNode)this.node).getOutputEntitiesUsed(this.requestContext.getProcessingContext().isValidationMode()).map(o -> o.getReferencedEntity()));
        this.LOGGER.debug("Source schema after is :{}", (Object)source.schema().treeString());
        this.LOGGER.debug("Schema for node " + ((SourceNode)this.node).getName());
        final DataFrameObject dataFrameObject = new DataFrameObject(source, String.join("\n", plan));
        this.sparkTranslatorState.addDataFrame(this.node, dataFrameObject);
    }
    
    @Override
    protected boolean canUseStarInSelect(final Function<Boolean, Stream<Entity>> entitiesUsedFormatter) {
        return false;
    }
}
