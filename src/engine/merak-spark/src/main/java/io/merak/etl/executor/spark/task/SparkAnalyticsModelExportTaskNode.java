package io.merak.etl.executor.spark.task;

import io.merak.etl.sdk.task.*;
import io.merak.etl.pipeline.dto.*;
import io.merak.etl.sdk.translator.*;
import java.util.*;

public class SparkAnalyticsModelExportTaskNode implements TaskNode
{
    private static final String RESULT_SET = "resultSet";
    protected final int priority = 1;
    protected final String id;
    private AnalyticsModelExportNode exportNode;
    private String parentNodeId;
    private TranslatorState sparkTranslatorState;
    
    public SparkAnalyticsModelExportTaskNode(final AnalyticsModelExportNode exportNode, final String parentNode, final TranslatorState sparkTranslatorState) {
        this.id = UUID.randomUUID().toString();
        this.exportNode = exportNode;
        this.parentNodeId = parentNode;
        this.sparkTranslatorState = sparkTranslatorState;
    }
    
    public AnalyticsModelExportNode getExportNode() {
        return this.exportNode;
    }
    
    public TranslatorState getTranslatorState() {
        return this.sparkTranslatorState;
    }
    
    public String getPredictorKey() {
        return this.parentNodeId;
    }
    
    public String getOutputKey() {
        return "resultSet";
    }
    
    public int getPriority() {
        return 1;
    }
    
    public String getId() {
        return this.id;
    }
    
    @Override
    public String toString() {
        return "AnalyticsModelExportTaskNode {exportNode =" + this.exportNode + ",parentNodeId = " + this.parentNodeId + "}";
    }
}
