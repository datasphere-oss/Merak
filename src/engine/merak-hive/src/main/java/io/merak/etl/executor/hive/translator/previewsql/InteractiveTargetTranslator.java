package io.merak.etl.executor.hive.translator.previewsql;

import io.merak.etl.executor.hive.task.*;
import io.merak.etl.pipeline.dto.*;
import io.merak.etl.sdk.task.*;
import io.merak.etl.translator.*;
import io.merak.etl.utils.config.*;

public class InteractiveTargetTranslator extends SqlTargetTranslator
{
    public InteractiveTargetTranslator(final TargetNode node, final TranslatorContext translatorContext) {
        super(node, translatorContext);
    }
    
    public TaskNode translate() {
        final boolean hasAuditColumns = this.injectAuditColumns();
        this.with();
        if (hasAuditColumns) {
            if (this.node.isOverwriteMode()) {
                this.selectAll().from(this.parentNodeName);
            }
            else if (this.node.isAppendMode()) {
                this.selectAll().from(String.format("(SELECT * FROM %s UNION ALL ", this.parentNodeName));
                this.select((PipelineNode)this.node, c -> String.format("%s%s", c.shouldExclude() ? "NULL " : "", c.getGivenName()));
                this.from(String.format("`%s`.`%s`", this.node.getSchemaName(), this.node.getTableName()));
                this.builder.append(") ").append(this.node.getName());
            }
            else {
                this.appendMergeStatement(this.parentNodeName);
            }
        }
        else {
            this.select((PipelineNode)this.node).from(this.parentNodeName);
        }
        if (this.node.isIntermediate()) {
            this.builder.insert(0, String.format("CREATE TABLE `%s`.`%s` STORED AS %s LOCATION '%s' AS\n", this.node.getSchemaName(), this.node.getTableName(), this.node.getStorageFormat(), this.node.getHdfsLocation()));
            this.limit(AwbConfigs.getInteractiveTableMaximumSize(this.requestContext));
            return (TaskNode)new QueryTaskNode(this.builder.toString());
        }
        this.limit();
        return (TaskNode)new QueryTaskNode(this.builder.toString());
    }
}
