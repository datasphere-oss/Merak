package io.merak.etl.executor.spark.translator;

import io.merak.etl.translator.*;
import io.merak.etl.executor.spark.executor.*;
import io.merak.etl.executor.spark.translator.dto.*;

import com.google.common.collect.*;
import java.util.*;

import org.apache.spark.sql.*;
import java.util.stream.*;
import io.merak.etl.pipeline.dto.*;

public class BatchSourceTranslator extends SourceTranslator
{
    public BatchSourceTranslator(final SourceNode node, final TranslatorContext translatorContext) {
        super(node, translatorContext);
    }
    
    @Override
    protected void generateDataFrame() {
        if (((SourceNode)this.node).isInjected()) {
            final String key = String.format("%s.%s", ((SourceNode)this.node).getSchemaName(), ((SourceNode)this.node).getTableName());
            final String executionStep = String.format("Getting cached dataset for cutpoint %s", key);
            this.sparkTranslatorState.addPlan(this.node, Lists.newArrayList((Object[])new String[] { executionStep }));
            Dataset<Row> source = DataFrameCache.getDataSet(key);
            source = this.select(this.node, this::sourceSelectFormatter, source, c -> ((SourceNode)this.node).getOutputEntitiesUsed(this.requestContext.getProcessingContext().isValidationMode()).map(o -> o.getReferencedEntity()));
            final DataFrameObject dataFrameObject = new DataFrameObject(source, executionStep);
            this.sparkTranslatorState.addDataFrame(this.node, dataFrameObject);
        }
        else {
            super.generateDataFrame();
        }
    }
}
