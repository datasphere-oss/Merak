package io.merak.etl.executor.spark.translator;

import io.merak.etl.sdk.translator.*;
import io.merak.etl.translator.*;
import java.util.*;
import com.google.common.collect.*;
import io.merak.etl.utils.date.*;
import org.apache.spark.sql.*;
import io.merak.etl.pipeline.dto.*;

public abstract class LoadTargetSparkTranslator extends SparkTranslator<LoadTarget> implements LoadTargetTranslator
{
    public LoadTargetSparkTranslator(final LoadTarget node, final TranslatorContext translatorContext) {
        super((PipelineNode)node, translatorContext);
    }
    
    protected Map<String, String> getAuditColumns() {
        final Map<String, String> auditColumns = (Map<String, String>)Maps.newLinkedHashMap();
        auditColumns.put("ZIW_ROW_ID".toLowerCase(), this.getRowIdExpr());
        auditColumns.put("ZIW_STATUS_FLAG".toLowerCase(), "'I'");
        auditColumns.put("ZIW_CREATED_TIMESTAMP".toLowerCase(), "current_timestamp()");
        auditColumns.put("ZIW_UPDATED_TIMESTAMP".toLowerCase(), "current_timestamp()");
        auditColumns.put("ZIW_IS_DELETED".toLowerCase(), "'false'");
        auditColumns.put("ZIW_TARGET_START_TIMESTAMP".toLowerCase(), DateUtil.getFormattedTimestampForHive(DateUtil.getCurrentTimestamp()));
        auditColumns.put("ZIW_TARGET_END_TIMESTAMP".toLowerCase(), DateUtil.getFormattedTimestampForHive(DateUtil.getCurrentTimestamp()));
        auditColumns.put("ZIW_ACTIVE".toLowerCase(), "true");
        return auditColumns;
    }
    
    protected String getRowIdExpr() {
        return "uuid()";
    }
    
    protected boolean injectAuditColumns() {
        return ((LoadTarget)this.node).addAuditColumns();
    }
    
    protected Dataset<Row> select(Dataset<Row> dataframe) {
        if (this.injectAuditColumns()) {
            final Map<String, String> expressions = this.getAuditColumns();
        }
        else {
            final Map<String, String> expressions = (Map<String, String>)Maps.newLinkedHashMap();
        }
        final Map<K, String> map;
        String auditExpression;
        dataframe = this.select((PipelineNode)this.node, c -> {
            if (!c.getOutputName((PipelineNode)this.node).equals(String.format("`%s`", "ziw_sec_p"))) {
                auditExpression = map.get(c.getGivenName());
                if (auditExpression == null) {
                    return this.defaultSelectFormatter(c);
                }
                else {
                    return String.format("%s %s", auditExpression, c.getOutputName((PipelineNode)this.node));
                }
            }
            else {
                return "";
            }
        }, dataframe);
        return dataframe;
    }
}
