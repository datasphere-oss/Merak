package io.merak.etl.executor.spark.executor;

import com.google.common.collect.Lists;
import io.merak.etl.context.RequestContext;
import io.merak.etl.pipeline.dto.Entity;
import io.merak.etl.pipeline.dto.PipelineNode;
import io.merak.etl.pipeline.dto.TargetNode;
import io.merak.etl.utils.config.AwbUtil;
import io.merak.etl.utils.date.DateUtil;
import io.merak.etl.executor.spark.translator.BatchTargetTranslator;

import java.util.List;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SCDHelper {
  private static final Logger LOGGER = LoggerFactory.getLogger(SCDHelper.class);
  
  private TargetNode targetNode;
  
  private BatchTargetTranslator translator;
  
  private RequestContext requestContext;
  
  public SCDHelper(TargetNode targetNode, BatchTargetTranslator translator) {
    this.targetNode = targetNode;
    this.translator = translator;
    this.requestContext = translator.getTranslatorContext().getRequestContext();
  }
  
  public List<String> getSCD2OldRecordColumns(TargetNode.SCD2GRANULARITY scd2Granularity) {
    List<String> columns = Lists.newArrayList();
    Stream<Entity> outputEntitiesStream = getOutputEntitiesStream();
    outputEntitiesStream.forEach(c -> {
          if (c.shouldExclude()) {
            columns.add(String.format("D.%s AS %s", new Object[] { c.getOutputName((PipelineNode)this.targetNode), c.getOutputName((PipelineNode)this.targetNode) }));
          } else if (!this.targetNode.getPrimaryPartitionColumns().contains(c) && !c.getOutputName((PipelineNode)this.targetNode).equals(String.format("`%s`", new Object[] { "ziw_sec_p" }))) {
            String exp;
            switch (c.getGivenName().toUpperCase()) {
              case "ZIW_TARGET_END_TIMESTAMP":
                columns.add(this.translator.getUpdatedRecordEndTimetamp(scd2Granularity.toString()) + " as " + "ZIW_TARGET_END_TIMESTAMP".toLowerCase());
                return;
              case "ZIW_ACTIVE":
                columns.add("false as " + "ZIW_ACTIVE".toLowerCase());
                return;
              case "ZIW_UPDATED_TIMESTAMP":
                columns.add(String.format("%s as %s", new Object[] { DateUtil.getFormattedTimestamp(this.translator.getCurrentTimestamp()), "ZIW_UPDATED_TIMESTAMP".toLowerCase() }));
                return;
              case "ZIW_STATUS_FLAG":
                exp = String.format("'U'as %s", new Object[] { c.getOutputName((PipelineNode)this.targetNode) });
                columns.add(exp);
                return;
            } 
            if (this.targetNode.isSCD1Column(c)) {
              columns.add(String.format("if(D.ziw_row_id IS NOT NULL,D.%s,T.%s)", new Object[] { c.getOutputName((PipelineNode)this.targetNode), c.getOutputName((PipelineNode)this.targetNode) }));
            } else {
              columns.add(String.format("T.%s as %s", new Object[] { c.getOutputName((PipelineNode)this.targetNode), c.getOutputName((PipelineNode)this.targetNode) }));
            } 
          } 
        });
    return columns;
  }
  
  public List<String> getOrderedEntityGivenNames() {
    List<String> selectColumnList = Lists.newArrayList();
    LOGGER.info("value is additive {}", this.targetNode.getSchemaChanged().toString());
    if (!this.targetNode.getSchemaChanged().booleanValue()) {
      for (Entity entity : this.targetNode.getOutputEntities()) {
        if (!this.targetNode.getPrimaryPartitionColumns().contains(entity) && !entity.getGivenName().equals("ziw_sec_p")) {
          LOGGER.info("select list is {}", entity.getGivenName());
          selectColumnList.add(String.format("`%s`", new Object[] { entity.getGivenName() }));
        } 
      } 
      for (Entity entity : this.targetNode.getPrimaryPartitionColumns())
        selectColumnList.add(entity.getGivenName()); 
      if (this.targetNode.isHiveCompatible())
        selectColumnList.add(String.format("`%s`", new Object[] { "ziw_sec_p" })); 
    } else {
      for (Entity entity : this.targetNode.getOutputEntities()) {
        selectColumnList.add(String.format("`%s`", new Object[] { entity.getGivenName() }));
      } 
    } 
    LOGGER.debug("Select column list {}", selectColumnList);
    return selectColumnList;
  }
  
  public List<String> getSCD2DefaultColumns(String tableAlias) {
    List<String> columns = Lists.newArrayList();
    Stream<Entity> outputEntitiesStream = getOutputEntitiesStream();
    outputEntitiesStream.forEach(c -> {
          if (c.shouldExclude()) {
            columns.add(String.format("D.%s AS %s", new Object[] { c.getOutputName((PipelineNode)this.targetNode), c.getOutputName((PipelineNode)this.targetNode) }));
          } else if (!this.targetNode.getPrimaryPartitionColumns().contains(c) && !c.getOutputName((PipelineNode)this.targetNode).equals(String.format("`%s`", new Object[] { "ziw_sec_p" }))) {
            columns.add(String.format("%s.%s as %s", new Object[] { tableAlias, c.getOutputName((PipelineNode)this.targetNode), c.getOutputName((PipelineNode)this.targetNode) }));
          } 
        });
    return columns;
  }
  
  public List<String> getSCD2DeltaTableColumns(TargetNode.SCD2GRANULARITY scd2Granularity) {
    List<String> columns = Lists.newArrayList();
    Stream<Entity> outputEntitiesStream = getOutputEntitiesStream();
    outputEntitiesStream
      .forEach(c -> {
          if (c.shouldExclude()) {
            columns.add(String.format("D.%s AS %s", new Object[] { c.getOutputName((PipelineNode)this.targetNode), c.getOutputName((PipelineNode)this.targetNode) }));
          } else if (!this.targetNode.getPrimaryPartitionColumns().contains(c) && !c.getOutputName((PipelineNode)this.targetNode).equals(String.format("`%s`", new Object[] { "ziw_sec_p" }))) {
            String subExp;
            String exp;
            String SCD2Updated;
            String columnChange;
            switch (c.getGivenName().toUpperCase()) {
              case "ZIW_TARGET_START_TIMESTAMP":
                columns.add(String.format("if(T.ziw_row_id is null,D.%s,if(not(%s),T.%s,D.%s)) as %s", new Object[] { c.getOutputName((PipelineNode)this.targetNode), isSCD2ColumnUpdated(false), c.getOutputName((PipelineNode)this.targetNode), c.getOutputName((PipelineNode)this.targetNode), c.getOutputName((PipelineNode)this.targetNode) }));
                return;
              case "ZIW_UPDATED_TIMESTAMP":
                subExp = isSCD1ColumnUpdated(false).isEmpty() ? String.format("T.%s", new Object[] { c.getOutputName((PipelineNode)this.targetNode) }) : String.format("(if(%s,D.%s,T.%s))", new Object[] { isSCD1ColumnUpdated(false), c.getOutputName((PipelineNode)this.targetNode), c.getOutputName((PipelineNode)this.targetNode) });
                columns.add(String.format("if(T.%s is null,D.%s,if(%s,D.%s,%s)) as %s", new Object[] { c.getOutputName((PipelineNode)this.targetNode), c.getOutputName((PipelineNode)this.targetNode), isSCD2ColumnUpdated(false), c.getOutputName((PipelineNode)this.targetNode), subExp, c.getOutputName((PipelineNode)this.targetNode) }));
                return;
              case "ZIW_STATUS_FLAG":
                exp = getTimeSimilarityUDFExp(scd2Granularity);
                SCD2Updated = isSCD2ColumnUpdated(false).isEmpty() ? "" : String.format("and %s", new Object[] { isSCD2ColumnUpdated(false) });
                columnChange = isSCD1ColumnUpdated(false).isEmpty() ? (isSCD2ColumnUpdated(false).isEmpty() ? "" : String.format("and %s", new Object[] { isSCD2ColumnUpdated(false) })) : String.format("and (%s or %s)", new Object[] { isSCD1ColumnUpdated(false), isSCD2ColumnUpdated(false) });
                columns.add(String.format("if(T.%s is not null %s %s ,'I',if(T.%s is not null %s ,'U',T.%s)) as %s", new Object[] { c.getOutputName((PipelineNode)this.targetNode), SCD2Updated, exp, c.getOutputName((PipelineNode)this.targetNode), columnChange, c.getOutputName((PipelineNode)this.targetNode), c.getOutputName((PipelineNode)this.targetNode) }));
                return;
            } 
            columns.add(String.format("D.%s as %s", new Object[] { c.getOutputName((PipelineNode)this.targetNode), c.getOutputName((PipelineNode)this.targetNode) }));
          } 
        });
    return columns;
  }
  
  public String getTimeSimilarityUDFExp(TargetNode.SCD2GRANULARITY scd2Granularity) {
    String exp = "";
    if (scd2Granularity == TargetNode.SCD2GRANULARITY.SECOND)
      return exp; 
    exp = String.format(" and not (iwtime_similarity(T.%s, D.%s, '%s'))", new Object[] { "ZIW_TARGET_START_TIMESTAMP"
          .toLowerCase(), "ZIW_TARGET_START_TIMESTAMP"
          .toLowerCase(), scd2Granularity });
    return exp;
  }
  
  public String isSCD2ColumnUpdated(boolean includeAnd) {
    String exp = includeAnd ? " and " : "";
    StringBuilder builder = new StringBuilder();
    Stream<Entity> outputEntitiesStream = getOutputEntitiesStream();
    outputEntitiesStream
      .forEach(c -> {
          if (!this.targetNode.getPrimaryPartitionColumns().contains(c) && !AwbUtil.isAuditColumn(c.getGivenName().toLowerCase()) && !this.targetNode.isSCD1Column(c))
            builder.append(String.format("(if(T.%s <=> D.%s,false,true)) or ", new Object[] { c.getOutputName(), c.getOutputName(), c.getOutputName(), c.getOutputName() })); 
        });
    if (builder.length() >= 4)
      return String.format("%s(%s)", new Object[] { exp, builder.substring(0, builder.length() - 4) }); 
    return builder.toString();
  }
  
  private String isSCD1ColumnUpdated(boolean includeAnd) {
    String exp = includeAnd ? " and " : "";
    StringBuilder builder = new StringBuilder();
    Stream<Entity> outputEntitiesStream = getOutputEntitiesStream();
    outputEntitiesStream
      .forEach(c -> {
          if (!this.targetNode.getPrimaryPartitionColumns().contains(c) && !AwbUtil.isAuditColumn(c.getGivenName().toLowerCase()) && this.targetNode.isSCD1Column(c))
            builder.append(String.format("(if(T.%s <=> D.%s,false,true)) or ", new Object[] { c.getOutputName(), c.getOutputName(), c.getOutputName(), c.getOutputName() })); 
        });
    if (builder.length() >= 4)
      return String.format("%s(%s)", new Object[] { exp, builder.substring(0, builder.length() - 4) }); 
    return builder.toString();
  }
  
  public List<String> getSCD1SelectColumns() {
    List<String> columns = Lists.newArrayList();
    this.targetNode.getOutputEntities()
      .forEach(c -> {
          if (c.shouldExclude()) {
            columns.add(String.format("D.%s AS %s", new Object[] { c.getOutputName((PipelineNode)this.targetNode), c.getOutputName((PipelineNode)this.targetNode) }));
          } else if (!this.targetNode.getPrimaryPartitionColumns().contains(c) && !c.getOutputName((PipelineNode)this.targetNode).equals(String.format("`%s`", new Object[] { "ziw_sec_p" }))) {
            switch (c.getGivenName()) {
              case "ziw_created_timestamp":
                columns.add("if(T.ziw_row_id IS NOT NULL, T.ziw_created_timestamp, D.ziw_created_timestamp) AS ziw_created_timestamp ");
                return;
              case "ziw_status_flag":
                columns.add("if(D.ziw_row_id IS NULL, T.ziw_status_flag, if(T.ziw_row_id IS NULL, 'I', 'U')) AS ziw_status_flag ");
                return;
            } 
            columns.add(String.format("if(D.ziw_row_id IS NULL, T.%s, D.%s) AS %s ", new Object[] { c.getOutputName((PipelineNode)this.targetNode), c.getOutputName((PipelineNode)this.targetNode), c.getOutputName((PipelineNode)this.targetNode) }));
          } 
        });
    return columns;
  }
  
  private Stream<Entity> getOutputEntitiesStream() {
    Stream<Entity> outputEntitiesStream;
    if (this.targetNode.isHiveCompatible()) {
      outputEntitiesStream = this.targetNode.getOutputEntities().stream();
    } else {
      outputEntitiesStream = this.targetNode.getOutputEntitiesUsed(this.requestContext.getProcessingContext().isValidationMode());
    } 
    return outputEntitiesStream;
  }
}
