package io.merak.etl.executor.spark.task.extensions;

import io.merak.etl.pipeline.dto.*;
import io.merak.etl.executor.spark.translator.*;
import io.merak.etl.executor.spark.translator.dto.*;

public class SparkCustomTargetBatchTaskNode extends SparkCustomTargetTaskNode
{
    public SparkCustomTargetBatchTaskNode(final CustomTargetTranslator translator, final CustomTargetNode node, final SparkTranslatorState sparkTranslatorState) {
        super(translator, node, sparkTranslatorState);
    }
}
