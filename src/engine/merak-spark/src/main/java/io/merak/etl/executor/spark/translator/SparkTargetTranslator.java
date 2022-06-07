package io.merak.etl.executor.spark.translator;

import io.merak.etl.translator.*;
import io.merak.etl.utils.date.*;
import io.merak.etl.executor.spark.utils.*;

import java.util.stream.*;

import org.apache.spark.sql.*;
import com.google.common.collect.*;
import io.merak.etl.utils.constants.*;
import scala.collection.*;
import io.merak.etl.pipeline.dto.*;
import java.util.*;

public abstract class SparkTargetTranslator extends SparkTranslator<TargetNode> implements TargetTranslator
{
    protected PipelineNode parentNode;
    protected String currentTimestamp;
    protected static final String PATH_OPTION = "path";
    
    public SparkTargetTranslator(final TargetNode node, final TranslatorContext translatorContext) {
        super((PipelineNode)node, translatorContext);
        this.parentNode = this.getParentNode((PipelineNode)node);
        this.currentTimestamp = DateUtil.getCurrentSystemTimestamp();
    }
    
    public boolean injectAuditColumns() {
        return !((TargetNode)this.node).isInjected();
    }
    
    public Map<String, String> getAuditColumns() {
        final Map<String, String> auditColumns = (Map<String, String>)Maps.newLinkedHashMap();
        auditColumns.put(AwbAuditColumns.ZIW_ROW_ID.name().toLowerCase(), this.getRowIdExpr());
        auditColumns.put(AwbAuditColumns.ZIW_TARGET_TIMESTAMP.name().toLowerCase(), "current_timestamp()");
        auditColumns.put(AwbAuditColumns.ZIW_STATUS_FLAG.name().toLowerCase(), "'I'");
        auditColumns.put(AwbAuditColumns.ZIW_EFFECTIVE_START_TIMESTAMP.name().toLowerCase(), "current_timestamp()");
        auditColumns.put(AwbAuditColumns.ZIW_EFFECTIVE_END_TIMESTAMP.name().toLowerCase(), this.getTargetEndExp());
        auditColumns.put(AwbAuditColumns.ZIW_ACTIVE.name().toLowerCase(), "true");
        auditColumns.put(AwbAuditColumns.ZIW_IS_DELETED.name().toLowerCase(), (((TargetNode)this.node).getDeletedRecordsColumn() != null) ? ((TargetNode)this.node).getDeletedRecordsColumn().getInputName() : "'false");
        return auditColumns;
    }
    
    public String getRowIdExpr() {
        return (((TargetNode)this.node).getNaturalKeyColumns().size() == 0) ? "uuid()" : String.format("hex(concat_ws(\":##:\", %s ))", ((TargetNode)this.node).getNaturalKeyColumns().stream().map(column -> String.format("nvl(cast(%s as string),\"NULL\")", column.getInputName())).collect(Collectors.joining(", ")));
    }
    
    public String getSecondaryColumnExpression() {
        return (((TargetNode)this.node).getSecondaryPartitionColumns().size() == 0) ? "uuid()" : String.format("hex(concat_ws(\":##:\", %s ))", ((TargetNode)this.node).getSecondaryPartitionColumns().stream().map(column -> String.format("nvl(cast(%s as string),\"NULL\")", column.getInputName())).collect(Collectors.joining(", ")));
    }
    
    public String getRowIdExprWithOutputColumnName() {
        return (((TargetNode)this.node).getSecondaryPartitionColumns().size() == 0) ? "uuid()" : String.format("hex(concat_ws(\":##:\", %s ))", ((TargetNode)this.node).getSecondaryPartitionColumns().stream().map(column -> String.format("nvl(cast(%s as string),\"NULL\")", column.getOutputName(column.getOwnerNode()))).collect(Collectors.joining(", ")));
    }
    
    public Dataset<Row> select(Dataset<Row> dataframe) {
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
        if (this.injectAuditColumns() && ((TargetNode)this.node).isHiveCompatible()) {
            final Column ziwSecP = SparkUtils.getColumnFromExp(this.spark, String.format("abs(hash( %s )) %% %d", this.getRowIdExprWithOutputColumnName(), ((TargetNode)this.node).getNumSecondaryPartitions()));
            dataframe = (Dataset<Row>)dataframe.withColumn("ziw_sec_p".toLowerCase(), ziwSecP);
        }
        return dataframe;
    }
    
    public String getCurrentTimestamp() {
        return this.currentTimestamp;
    }
    
    private String getTargetEndExp() {
        return "CAST (unix_timestamp('9999-09-09 09:09:09','yyyy-MM-dd HH:mm:ss') AS TIMESTAMP )";
    }
    
    protected DataFrameWriter getWriterNonCompatible(final TargetNode targetNode, final Dataset<Row> dataset) {
        final List<String> partitionColumns = (List<String>)targetNode.getPrimaryPartitionColumns().stream().map(i -> i.getGivenName()).collect(Collectors.toList());
        DataFrameWriter<Row> dfWriter = (DataFrameWriter<Row>)dataset.write();
        if (!partitionColumns.isEmpty()) {
            dfWriter = (DataFrameWriter<Row>)dfWriter.partitionBy((String[])partitionColumns.toArray());
        }
        try {
            List<String> buckets = (List<String>)Lists.newArrayList();
            if (((TargetNode)this.node).secondaryColumnsExist()) {
                buckets = (List<String>)targetNode.getSecondaryPartitionColumns().stream().map(i -> i.getGivenName()).collect(Collectors.toList());
            }
            else {
                buckets.add("ZIW_ROW_ID".toLowerCase());
            }
            final int numBuckets = targetNode.getNumSecondaryPartitions();
            List<String> bucketColumns = (List<String>)Lists.newArrayList();
            if (!buckets.isEmpty()) {
                final String firstBucketCol = buckets.get(0);
                if (buckets.size() > 1) {
                    bucketColumns = buckets.subList(1, buckets.size());
                }
                if (!((TargetNode)this.node).getStorageFormat().equalsIgnoreCase(AwbConstants.FileFormat.DELTA.name())) {
                    dfWriter = (DataFrameWriter<Row>)dfWriter.bucketBy(numBuckets, firstBucketCol, (Seq)SparkUtils.getStringSeqFromList(bucketColumns));
                }
            }
        }
        catch (Throwable e) {
            this.LOGGER.error("Error at buckets in ", e);
        }
        return dfWriter;
    }
    
    public String getOutputNameForProperty(final Entity input) {
        final Optional<Entity> output = ((TargetNode)this.node).getOutputEntitiesUsed(true).filter(out -> out.getReferencedEntity() != null && !out.getReferenceType().isDerivation() && out.getReferencedEntity().equals((Object)input)).findAny();
        return output.isPresent() ? output.get().getOutputName() : String.format("`%s`", input.getGivenName());
    }
}
