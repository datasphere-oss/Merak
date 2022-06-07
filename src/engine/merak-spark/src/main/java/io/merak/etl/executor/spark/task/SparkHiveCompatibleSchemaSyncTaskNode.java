package io.merak.etl.executor.spark.task;

import io.merak.etl.task.impl.*;
import io.merak.etl.pipeline.dto.*;
import io.merak.etl.sdk.task.*;

public class SparkHiveCompatibleSchemaSyncTaskNode extends AbstractTaskNode
{
    private final TargetNode targetNode;
    private final TaskNode targetTaskNode;
    
    public SparkHiveCompatibleSchemaSyncTaskNode(final TargetNode targetNode, final TaskNode taskNode) {
        this.targetNode = targetNode;
        this.targetTaskNode = taskNode;
    }
    
    public TargetNode getTargetNode() {
        return this.targetNode;
    }
    
    public TaskNode getTargetTaskNode() {
        return this.targetTaskNode;
    }
}
