package io.merak.etl.executor.spark.task;

import io.merak.etl.task.impl.*;
import io.merak.etl.pipeline.dto.*;

public class SparkCopySampleTaskNode extends AbstractTaskNode
{
    private final String pipelineId;
    private final SourceNode sourceNode;
    
    public SparkCopySampleTaskNode(final String pipelineId, final SourceNode sourceNode) {
        this.pipelineId = pipelineId;
        this.sourceNode = sourceNode;
    }
    
    public String getPipelineId() {
        return this.pipelineId;
    }
    
    public SourceNode getSourceNode() {
        return this.sourceNode;
    }
    
    public String toString() {
        return "CopySampleTaskNode{pipelineId='" + this.pipelineId + '\'' + ", sourceNode=" + this.sourceNode.getId() + ", priority=" + 1 + ", id='" + this.id + '\'' + '}';
    }
}
