package io.merak.etl.executor.hive.translator;

import io.merak.etl.executor.hive.task.*;
import io.merak.etl.pipeline.nodes.*;
import io.merak.etl.sdk.task.*;
import io.merak.etl.translator.*;

import com.google.common.base.*;

public class DropTableTranslator extends SqlTranslator
{
    private final DropTableNode node;
    
    public DropTableTranslator(final DropTableNode node, final TranslatorContext translatorContext) {
        super(translatorContext);
        this.node = node;
    }
    
    public TaskNode translate() {
        Preconditions.checkState(this.builder.length() == 0, (Object)"builder is not empty!");
        this.builder.append(String.format("DROP TABLE IF EXISTS `%s`.`%s` PURGE", this.node.getSchemaName(), this.node.getTableName()));
        return (TaskNode)new QueryTaskNode(this.builder.toString());
    }
}
