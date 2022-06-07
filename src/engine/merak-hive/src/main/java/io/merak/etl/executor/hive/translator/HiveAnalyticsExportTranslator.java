package io.merak.etl.executor.hive.translator;

import io.merak.etl.executor.hive.task.*;
import io.merak.etl.pipeline.dto.*;
import io.merak.etl.sdk.task.*;
import io.merak.etl.sdk.translator.*;
import io.merak.etl.translator.*;

public class HiveAnalyticsExportTranslator extends SinkTranslator
{
    protected AnalyticsModelExportNode node;
    
    public HiveAnalyticsExportTranslator(final AnalyticsModelExportNode node, final TranslatorContext translatorContext) {
        super(translatorContext);
        this.node = node;
    }
    
    public TaskNode translate() {
        this.start(this.node.getId()).select((PipelineNode)this.node).from(this.getParent((PipelineNode)this.node)).end();
        TaskNode taskNode = this.endTranslation((PipelineNode)this.node);
        if (taskNode == null) {
            taskNode = (TaskNode)new AnalyticsModelExportTaskNode(this.node, this.getParent((PipelineNode)this.node), null);
        }
        return taskNode;
    }
}
