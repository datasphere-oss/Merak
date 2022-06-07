package io.merak.etl.executor.hive.task;

import io.merak.etl.pipeline.dto.*;
import io.merak.etl.sdk.task.*;
import io.merak.etl.task.impl.*;

public class HiveSchemaSyncTaskNode extends AbstractTaskNode
{
    private final TargetNode targetNode;
    private final TaskNode targetTaskNode;
    
    public HiveSchemaSyncTaskNode(final TargetNode targetNode, final TaskNode taskNode) {
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
