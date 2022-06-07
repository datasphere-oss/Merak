package io.merak.etl.executor.hive.task;

import io.merak.etl.executor.hive.translator.*;
import io.merak.etl.pipeline.dto.*;
import io.merak.etl.sdk.pipeline.*;
import io.merak.etl.sdk.task.*;
import io.merak.etl.sdk.translator.*;
import io.merak.etl.translator.*;

public class BatchQueryTaskNode extends QueryTaskNode implements SinkTaskNode
{
    private final TargetNode targetNode;
    private final BatchTargetTranslator targetTranslator;
    
    public BatchQueryTaskNode(final BatchTargetTranslator targetTranslator, final TargetNode targetNode, final String query) {
        super(query);
        this.targetTranslator = targetTranslator;
        this.targetNode = targetNode;
    }
    
    public TargetNode getSink() {
        return this.targetNode;
    }
    
    public TranslatorState getTranslatorState() {
        return null;
    }
    
    public TargetTranslator getSinkTranslator() {
        return (TargetTranslator)this.targetTranslator;
    }
}
