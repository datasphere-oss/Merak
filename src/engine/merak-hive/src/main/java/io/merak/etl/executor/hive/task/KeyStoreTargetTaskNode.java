package io.merak.etl.executor.hive.task;

import io.merak.etl.executor.hive.translator.*;
import io.merak.etl.pipeline.dto.*;
import io.merak.etl.sdk.translator.*;
import io.merak.etl.task.impl.*;

public class KeyStoreTargetTaskNode extends QueryTaskNode implements LoadTargetTaskNode
{
    private final MapRDBKeyStoreTarget keyValueTargetNode;
    private final MapRDBHiveTranslator targetTranslator;
    
    public KeyStoreTargetTaskNode(final MapRDBKeyStoreTarget keyValueTargetNode, final MapRDBHiveTranslator targetTranslator, final String query) {
        super(query);
        this.keyValueTargetNode = keyValueTargetNode;
        this.targetTranslator = targetTranslator;
    }
    
    public KeyValueTargetNode getKeyValueTargetNode() {
        return (KeyValueTargetNode)this.keyValueTargetNode;
    }
    
    public LoadTarget getSink() {
        return (LoadTarget)this.keyValueTargetNode;
    }
    
    public LoadTargetTranslator getSinkTranslator() {
        return (LoadTargetTranslator)this.targetTranslator;
    }
    
    public TranslatorState getTranslatorState() {
        return null;
    }
}
