package io.merak.etl.executor.spark.translator;

import io.merak.etl.translator.*;
import org.slf4j.*;
import io.merak.etl.dispatcher.job.*;
import io.merak.etl.job.dispatcher.*;
import io.merak.etl.utils.classloader.*;
import io.merak.etl.executor.spark.translator.dto.*;
import io.merak.etl.executor.spark.utils.*;

import java.util.function.*;
import com.google.common.base.*;
import com.google.common.collect.*;
import org.apache.spark.api.java.function.*;
import org.apache.spark.sql.catalyst.encoders.*;
import org.apache.spark.sql.types.*;
import io.merak.etl.pipeline.dto.*;
import java.util.stream.*;
import java.math.*;
import java.util.*;
import org.apache.spark.sql.*;

public class LookupTranslator extends SparkTranslator<LookupNode>
{
    private final Logger LOGGER;
    private static final String RULE_ID_COLUMN = "ziw_rule_id";
    
    public LookupTranslator(final LookupNode node, final TranslatorContext translatorContext) {
        super((PipelineNode)node, translatorContext);
        this.LOGGER = LoggerFactory.getLogger((Class)LookupTranslator.class);
        if (PipelineUtils.getDispatcherJobType(this.requestContext) == JobType.NATIVE_JOB) {
            ExtensionUtils.getInstance().addJarsToSparkContext((PipelineNode)node, this.spark, this.requestContext);
        }
    }
    
    private Dataset<Row> getLookupDataset() {
        Dataset<Row> rulesDataset = null;
        this.LOGGER.debug("Fetching lookup table into dataframe...");
        if (((LookupNode)this.node).getProperties().getSourceType() == LookupNode.SourceType.table) {
            rulesDataset = (Dataset<Row>)this.spark.sql(String.format("select * from `%s`.`%s`", ((LookupNode)this.node).getProperties().getSchemaName(), ((LookupNode)this.node).getProperties().getTableName()));
        }
        else {
            rulesDataset = (Dataset<Row>)this.spark.read().format("csv").option("header", "true").load(((LookupNode)this.node).getProperties().getRulePath());
        }
        rulesDataset = (Dataset<Row>)rulesDataset.withColumn("ziw_rule_id", functions.monotonically_increasing_id());
        return rulesDataset;
    }
    
    public void validateRuleDatasetColumns(final Dataset ruleDataset) {
        final String[] columns = ruleDataset.columns();
        final ArrayList<String> columnList = new ArrayList<String>(Arrays.asList(columns));
        Preconditions.checkState(columnList.stream().anyMatch((Predicate<? super Object>)((LookupNode)this.node).getProperties().getDeriveColumnName()::equalsIgnoreCase), (Object)"Given derive column is not present in the lookup source");
        if (((LookupNode)this.node).getProperties().getPriorityColumnName() != null) {
            Preconditions.checkState(columnList.stream().anyMatch((Predicate<? super Object>)((LookupNode)this.node).getProperties().getPriorityColumnName()::equalsIgnoreCase), (Object)"Given priority column is not present in the lookup source");
        }
        for (final LookupNode.RuleColumn rule : ((LookupNode)this.node).getProperties().getRangeColumns()) {
            Preconditions.checkState(columnList.stream().anyMatch((Predicate<? super Object>)rule.getRangeStartColumn()::equalsIgnoreCase), "Given range start column %s is missing in lookup source ", new Object[] { rule.getRangeStartColumn() });
            Preconditions.checkState(columnList.stream().anyMatch((Predicate<? super Object>)rule.getRangeEndColumn()::equalsIgnoreCase), "Given range end column %s is missing in lookup source ", new Object[] { rule.getRangeEndColumn() });
        }
        for (final Map.Entry<String, String> entry : ((LookupNode)this.node).getProperties().getLookupColumns().entrySet()) {
            Preconditions.checkState(columnList.stream().anyMatch((Predicate<? super Object>)(String)entry.getValue()::equalsIgnoreCase), "Given lookup column %s is missing in lookup source ", new Object[] { entry.getValue() });
        }
    }
    
    @Override
    protected void generateDataFrame() {
        final PipelineNode parent = this.getParentNode((PipelineNode)this.node);
        final DataFrameObject incomingDFO = this.sparkTranslatorState.getDataFrame(parent);
        final Dataset inputDataset = incomingDFO.getDataset();
        final Dataset<Row> rulesDataset = this.getLookupDataset();
        this.validateRuleDatasetColumns(rulesDataset);
        final List<String> plan = (List<String>)Lists.newArrayList();
        plan.add(String.format("Node : %s", ((LookupNode)this.node).getId()));
        final ExpressionEncoder<Row> encoder = (ExpressionEncoder<Row>)RowEncoder.apply(this.getOutputSchema(inputDataset.schema()));
        final Hashtable rulesColNameToId = this.loadColNamesToId(rulesDataset, false);
        final Hashtable rulesColIdToName = this.loadColIdToNames(rulesDataset);
        final Hashtable inputColNameToId = this.loadColNamesToId(inputDataset, true);
        final Hashtable rulesRangedColumns = this.loadRangeColumns(rulesDataset);
        final Hashtable rangedColumnNameToInputName = this.loadRangedNameToInputName(rulesRangedColumns);
        final List<Row> listrows = (List<Row>)rulesDataset.collectAsList();
        final List<LookupNode.RangeRule> rules = this.getRules(listrows, rulesColNameToId, rulesColIdToName, inputColNameToId, rulesRangedColumns, ((LookupNode)this.node).getProperties().getPriorityColumnName(), "ziw_rule_id", ((LookupNode)this.node).getProperties().getDeriveColumnName(), ((LookupNode)this.node).getProperties().isPriorityIgnore());
        plan.add(String.format("Exact lookup Column : %s", ((LookupNode)this.node).getProperties().getLookupColumns()));
        final List<String> rangeColumnsList = (List<String>)Lists.newArrayList();
        ((LookupNode)this.node).getProperties().getRangeColumns().forEach(i -> rangeColumnsList.add(String.format("input_column : %s, range_start_column : %s, range_end_column : %s", i.getInputColumnName(), i.getRangeStartColumn(), i.getRangeEndColumn())));
        plan.add(String.format("Range columns : %s", rangeColumnsList));
        plan.add(String.format("Derive Column name : %s", ((LookupNode)this.node).getProperties().getDeriveColumnName()));
        final Map<String, LookupNode.RangeRule> idToRulesMap = this.getIdToRulesMap(rules);
        final List<Integer> parsingOrderForRules = this.getParsingOrderForRules(rulesColIdToName, rulesColNameToId, inputColNameToId, rangedColumnNameToInputName, ((LookupNode)this.node).getProperties().getPriorityColumnName(), "ziw_rule_id", ((LookupNode)this.node).getProperties().getDeriveColumnName());
        final List<Set> pair = (List<Set>)this.getParsingOrderForInputs(rulesColIdToName, inputColNameToId, rangedColumnNameToInputName, parsingOrderForRules);
        final Set<Integer> orderedInputColumnIndexes = pair.get(0);
        final Set<String> orderedInputColumnNames = pair.get(1);
        final List<NavigableMap<String, LookupNode.RuleId>> valueMapsForRules = (List<NavigableMap<String, LookupNode.RuleId>>)this.getValueMapsForRules(orderedInputColumnNames, rules, rulesColIdToName, rangedColumnNameToInputName);
        final Dataset output = inputDataset.map((MapFunction)new RddFunction(valueMapsForRules, idToRulesMap, orderedInputColumnIndexes, ((LookupNode)this.node).getProperties().getDefaultValue()), (Encoder)encoder);
        this.LOGGER.trace("Look up Schema is : {}", (Object)output.schema().treeString());
        final DataFrameObject lookupDFO = new DataFrameObject((Dataset<Row>)output, String.join("\n", plan));
        this.sparkTranslatorState.addDataFrame(this.node, lookupDFO);
    }
    
    public StructType getOutputSchema(final StructType inputschema) {
        final StructField[] outputfields = new StructField[inputschema.size() + 2];
        final StructField[] fields = inputschema.fields();
        final int numFields = fields.length;
        for (int i = 0; i < inputschema.size(); ++i) {
            outputfields[i] = fields[i];
        }
        outputfields[numFields] = new StructField("ziw_rule_id", DataTypes.StringType, true, Metadata.empty());
        outputfields[numFields + 1] = new StructField(((LookupNode)this.node).getProperties().getDerivations().get(0).getDerivedField().getOutputName((PipelineNode)this.node), DataTypes.StringType, true, Metadata.empty());
        final StructType outputschema = new StructType(outputfields);
        return outputschema;
    }
    
    public Hashtable loadColNamesToId(final Dataset input, final boolean isInput) {
        final Hashtable colNameToId = new Hashtable();
        final String[] columns = input.columns();
        for (int i = 0; i < columns.length; ++i) {
            final String outputName = columns[i];
            if (isInput) {
                String colName = null;
                for (final Entity col : ((LookupNode)this.node).getInputEntities()) {
                    if (col.getOutputName().equalsIgnoreCase(outputName)) {
                        colName = col.getGivenName().toLowerCase();
                    }
                }
                if (colName == null) {
                    this.LOGGER.error("No entity found with output name as {}", (Object)outputName);
                }
                colNameToId.put(colName, i);
            }
            else {
                colNameToId.put(columns[i].toLowerCase(), i);
            }
        }
        return colNameToId;
    }
    
    public Hashtable loadColIdToNames(final Dataset input) {
        final Hashtable colIdToName = new Hashtable();
        final String[] columns = input.columns();
        for (int i = 0; i < columns.length; ++i) {
            colIdToName.put(i, columns[i].toLowerCase());
        }
        return colIdToName;
    }
    
    public Hashtable loadRangeColumns(final Dataset input) {
        final Hashtable<String, LookupNode.RangeColumn> rangedColumnsList = new Hashtable<String, LookupNode.RangeColumn>();
        final String[] columns = input.columns();
        final Map<String, String> minToValue = new HashMap<String, String>();
        final Map<String, String> maxToValue = new HashMap<String, String>();
        final Map<String, String> map;
        final Map<String, String> map2;
        ((LookupNode)this.node).getProperties().getRangeColumns().forEach(i -> {
            map.put(i.getRangeStartColumn().toLowerCase(), i.getInputColumnName().toLowerCase());
            map2.put(i.getRangeEndColumn().toLowerCase(), i.getInputColumnName().toLowerCase());
            return;
        });
        for (int j = 0; j < columns.length; ++j) {
            final String columnName = columns[j].toLowerCase();
            if (minToValue.containsKey(columnName)) {
                final String s = minToValue.get(columnName);
                LookupNode.RangeColumn r = new LookupNode.RangeColumn();
                if (rangedColumnsList.containsKey(s)) {
                    r = rangedColumnsList.get(s);
                }
                r.setColumnName(s);
                r.setMinIndex(j);
                r.setMinName(columnName);
                rangedColumnsList.put(r.getColumnName(), r);
            }
            else if (maxToValue.containsKey(columnName)) {
                final String s = maxToValue.get(columnName);
                LookupNode.RangeColumn r = new LookupNode.RangeColumn();
                if (rangedColumnsList.containsKey(s)) {
                    r = rangedColumnsList.get(s);
                }
                r.setColumnName(s);
                r.setMaxIndex(j);
                r.setMaxName(columnName);
                rangedColumnsList.put(r.getColumnName(), r);
            }
            else {
                String inputColumnName = columnName;
                final LookupNode.RangeColumn r = new LookupNode.RangeColumn();
                for (final Map.Entry<String, String> entry : ((LookupNode)this.node).getProperties().getLookupColumns().entrySet()) {
                    if (entry.getValue().equalsIgnoreCase(columnName)) {
                        inputColumnName = entry.getKey().toLowerCase();
                        break;
                    }
                }
                r.setColumnName(inputColumnName);
                r.setMaxIndex(j);
                r.setMaxName(columnName);
                r.setMinIndex(j);
                r.setMinName(columnName);
                rangedColumnsList.put(r.getColumnName(), r);
            }
        }
        return rangedColumnsList;
    }
    
    public Hashtable loadRangedNameToInputName(final Hashtable<String, LookupNode.RangeColumn> rangedColumns) {
        final Hashtable<String, String> rangedNameToInputName = new Hashtable<String, String>();
        final String key;
        final LookupNode.RangeColumn value;
        final Hashtable<String, String> hashtable;
        rangedColumns.entrySet().forEach(entry -> {
            key = entry.getKey();
            value = (LookupNode.RangeColumn)entry.getValue();
            hashtable.put(value.getMinName(), key);
            hashtable.put(value.getMaxName(), key);
            return;
        });
        return rangedNameToInputName;
    }
    
    public List<LookupNode.RangeRule> getRules(final List<Row> ruleslist, final Hashtable colNameToId, final Hashtable colIdToName, final Hashtable inputColNameToId, final Hashtable rulesRangedColumns, final String priority, final String ruleId, final String result, final boolean priorityIgnore) {
        final List<LookupNode.RangeRule> rules = ruleslist.stream().map(row -> {
            try {
                return this.rowToRule(row, colNameToId, colIdToName, inputColNameToId, rulesRangedColumns, priority, ruleId, result, priorityIgnore);
            }
            catch (Exception e) {
                e.printStackTrace();
                this.LOGGER.warn("Row to rule conversion failed with exception {}", (Throwable)e);
                return null;
            }
        }).collect((Collector<? super Object, ?, List<LookupNode.RangeRule>>)Collectors.toList());
        return rules;
    }
    
    public LookupNode.RangeRule rowToRule(final Row row, final Hashtable colNameToId, final Hashtable colIdToName, final Hashtable inputColNameToId, final Hashtable<String, LookupNode.RangeColumn> rulesRangedColumns, final String priorityColumn, final String ruleIdColumn, final String resultColumn, final boolean priorityIgnore) throws Exception {
        final LookupNode.RangeRule r = new LookupNode.RangeRule();
        final String finalPriorityColumn = (priorityColumn != null) ? priorityColumn.toLowerCase() : null;
        final String finalRuleIdColumn = (ruleIdColumn != null) ? ruleIdColumn.toLowerCase() : null;
        final String finalResultColumn = (resultColumn != null) ? resultColumn.toLowerCase() : null;
        final Integer priorityIndex = (finalPriorityColumn != null && colNameToId.containsKey(finalPriorityColumn)) ? colNameToId.get(finalPriorityColumn) : -1;
        final Integer ruleIdIndex = colNameToId.get(finalRuleIdColumn);
        final Integer resultIndex = colNameToId.get(finalResultColumn);
        final String key;
        final LookupNode.RangeColumn rc;
        final String s;
        final Integer n;
        Object o;
        int p;
        String priorityString;
        final LookupNode.RangeRule rangeRule;
        final String s2;
        final Integer n2;
        Object o2;
        final String s3;
        final Integer n3;
        Object o3;
        Object minValue;
        Object maxValue;
        String minValueString;
        String maxValueString;
        rulesRangedColumns.entrySet().forEach(entry -> {
            key = entry.getKey();
            rc = (LookupNode.RangeColumn)entry.getValue();
            if (key.equalsIgnoreCase(s)) {
                if (!priorityIgnore) {
                    if (!priorityIgnore) {
                        o = row.get((int)n);
                        p = -1;
                        if (o != null) {
                            priorityString = o.toString();
                            if (!priorityString.isEmpty()) {
                                p = Integer.parseInt(priorityString);
                            }
                        }
                        rangeRule.setPriority(Integer.valueOf(p));
                    }
                }
            }
            else if (key.equalsIgnoreCase(s2)) {
                o2 = row.get((int)n2);
                rangeRule.setRuleId(o2.toString());
            }
            else if (key.equalsIgnoreCase(s3)) {
                o3 = row.get((int)n3);
                rangeRule.setResult(o3.toString());
            }
            else if (!(!inputColNameToId.containsKey(key))) {
                minValue = row.get(rc.getMinIndex());
                maxValue = row.get(rc.getMaxIndex());
                minValueString = ((minValue != null) ? minValue.toString() : null);
                maxValueString = ((maxValue != null) ? maxValue.toString() : null);
                rangeRule.setRangedValue(key, minValueString, maxValueString);
            }
            return;
        });
        return r;
    }
    
    public Map<String, LookupNode.RangeRule> getIdToRulesMap(final List<LookupNode.RangeRule> rules) {
        final Map<String, LookupNode.RangeRule> rulesMap = new HashMap<String, LookupNode.RangeRule>();
        rules.forEach(rule -> rulesMap.put(rule.getRuleId(), rule));
        return rulesMap;
    }
    
    public List<Integer> getParsingOrderForRules(final Hashtable colIdToName, final Hashtable colNameToId, final Hashtable inputColNameToId, final Hashtable rangedColumnNameToInputName, String priorityColumn, String ruleIdColumn, String resultColumn) {
        final List<Integer> sortedKeys = new ArrayList<Integer>(colIdToName.keySet());
        Collections.sort(sortedKeys);
        priorityColumn = ((priorityColumn != null) ? priorityColumn.toLowerCase() : null);
        ruleIdColumn = ((ruleIdColumn != null) ? ruleIdColumn.toLowerCase() : null);
        resultColumn = ((resultColumn != null) ? resultColumn.toLowerCase() : null);
        final Integer priorityIndex = (priorityColumn != null && colNameToId.containsKey(priorityColumn)) ? colNameToId.get(priorityColumn) : -1;
        final Integer ruleIdIndex = colNameToId.get(ruleIdColumn);
        final Integer resultIndex = colNameToId.get(resultColumn);
        this.LOGGER.debug("Removing priorityIndex : {}, ruleIdIndex : {}, resultIndex : {}", new Object[] { priorityIndex, ruleIdIndex, resultIndex });
        sortedKeys.remove(priorityIndex);
        sortedKeys.remove(ruleIdIndex);
        sortedKeys.remove(resultIndex);
        final List<Integer> removeIndexes = new ArrayList<Integer>();
        for (final Integer i : sortedKeys) {
            final String colName = colIdToName.get(i);
            boolean hasInputMapping = false;
            for (final Map.Entry<String, String> entry : ((LookupNode)this.node).getProperties().getLookupColumns().entrySet()) {
                if (entry.getValue().equalsIgnoreCase(colName)) {
                    hasInputMapping = true;
                    break;
                }
            }
            if (!hasInputMapping) {
                if (!rangedColumnNameToInputName.containsKey(colName)) {
                    removeIndexes.add(i);
                }
                else {
                    final String inputColName = rangedColumnNameToInputName.get(colName);
                    if (inputColNameToId.containsKey(inputColName.toLowerCase()) && ((LookupNode)this.node).isRuleExist(inputColName, colName)) {
                        continue;
                    }
                    removeIndexes.add(i);
                }
            }
        }
        for (final Integer i : removeIndexes) {
            sortedKeys.remove(i);
        }
        return sortedKeys;
    }
    
    public List getParsingOrderForInputs(final Hashtable rulesColIdToName, final Hashtable inputColNameToId, final Hashtable rangedColumnNameToInputName, final List<Integer> orderedColumnsRules) {
        final Set<Integer> orderedInputColumnIndexes = new LinkedHashSet<Integer>();
        final Set<String> orderedInputColumnNames = new LinkedHashSet<String>();
        for (final Integer i : orderedColumnsRules) {
            final String ruleColName = rulesColIdToName.get(i);
            final String inputMapping = this.getKeyForValue(((LookupNode)this.node).getProperties().getLookupColumns(), ruleColName);
            Integer inputIndex = null;
            if (inputMapping != null) {
                inputIndex = inputColNameToId.get(inputMapping);
            }
            if (inputIndex == null) {
                if (!rangedColumnNameToInputName.containsKey(ruleColName)) {
                    continue;
                }
                final String inputColName = rangedColumnNameToInputName.get(ruleColName);
                if (!inputColNameToId.containsKey(inputColName.toLowerCase())) {
                    continue;
                }
                inputIndex = inputColNameToId.get(inputColName.toLowerCase());
                orderedInputColumnIndexes.add(inputIndex);
                orderedInputColumnNames.add(inputColName.toLowerCase());
            }
            else {
                if (inputIndex == null) {
                    continue;
                }
                orderedInputColumnIndexes.add(inputIndex);
                orderedInputColumnNames.add(inputMapping);
            }
        }
        return Arrays.asList(orderedInputColumnIndexes, orderedInputColumnNames);
    }
    
    public String getKeyForValue(final Map<String, String> map, final String value) {
        for (final Map.Entry<String, String> entry : map.entrySet()) {
            if (entry.getValue().equalsIgnoreCase(value)) {
                return entry.getKey();
            }
        }
        return null;
    }
    
    public List getValueMapsForRules(final Set<String> orderedInputColumnNames, final List<LookupNode.RangeRule> rules, final Hashtable rulesColIdToName, final Hashtable rangedColumnNameToInputName) {
        final List listMaps = new ArrayList();
        for (final String columnName : orderedInputColumnNames) {
            final NavigableMap<String, LookupNode.RuleId> columnValuesToRules = this.getListRulesMap(rules, columnName, rulesColIdToName, rangedColumnNameToInputName);
            listMaps.add(columnValuesToRules);
        }
        return listMaps;
    }
    
    public NavigableMap<String, LookupNode.RuleId> getListRulesMap(final List<LookupNode.RangeRule> rules, final String columnName, final Hashtable colIdToName, final Hashtable rangedColumnNameToInputName) {
        final NavigableMap<String, LookupNode.RuleId> valueToRules = new TreeMap<String, LookupNode.RuleId>();
        for (int i = 0; i < rules.size(); ++i) {
            final LookupNode.RangeRule r = rules.get(i);
            final String minValue = (r.getMinimumValue(columnName) == null) ? "" : r.getMinimumValue(columnName);
            final String maxValue = (r.getMaximumValue(columnName) == null) ? "" : r.getMaximumValue(columnName);
            if (!valueToRules.containsKey(minValue)) {
                final List<String> ruleIds = new ArrayList<String>();
                ruleIds.add(r.getRuleId());
                final LookupNode.RuleId ruleId = new LookupNode.RuleId();
                ruleId.setRuleIds((List)ruleIds);
                ruleId.setUpperValue(maxValue);
                valueToRules.put(minValue, ruleId);
            }
            else {
                final LookupNode.RuleId ruleId2 = valueToRules.get(minValue);
                ruleId2.addToRuleIds(r.getRuleId());
                ruleId2.setUpperValue(maxValue);
            }
        }
        return valueToRules;
    }
    
    public static class RddFunction implements MapFunction<Row, Row>
    {
        private static final long serialVersionUID = -4332903997027358601L;
        private Set<Integer> orderedColumnsInput;
        private List listmaps;
        private Map<String, LookupNode.RangeRule> rules;
        private SparkSession spark;
        private String defaultResult;
        
        RddFunction(final List listmaps, final Map<String, LookupNode.RangeRule> rules, final Set<Integer> orderedColumnsInput, final String defaultResult) {
            this.listmaps = listmaps;
            this.rules = rules;
            this.orderedColumnsInput = orderedColumnsInput;
            this.defaultResult = defaultResult;
        }
        
        public String getBestRuleId(final HashSet<String> ruleIds) {
            String bestRuleId = "";
            int bestPriority = -100;
            int bestRowNumber = -100;
            for (final String ruleId : ruleIds) {
                final LookupNode.RangeRule rule = this.rules.get(ruleId);
                if (rule.getPriority() > bestPriority) {
                    bestPriority = rule.getPriority();
                    bestRuleId = rule.getRuleId();
                    bestRowNumber = rule.getRowNumber();
                }
                else {
                    if (rule.getPriority() != bestPriority || bestRowNumber <= rule.getRowNumber()) {
                        continue;
                    }
                    bestPriority = rule.getPriority();
                    bestRuleId = rule.getRuleId();
                    bestRowNumber = rule.getRowNumber();
                }
            }
            return bestRuleId;
        }
        
        public Row call(final Row row) throws Exception {
            int index = 0;
            final List ruleSets = new ArrayList();
            for (final Integer colIndex : this.orderedColumnsInput) {
                final NavigableMap<String, LookupNode.RuleId> valueToRuleIds = this.listmaps.get(index);
                final Object o = row.get((int)colIndex);
                final String val = (o != null) ? o.toString() : "";
                final HashSet<String> set = new HashSet<String>();
                if (valueToRuleIds.containsKey(val)) {
                    final LookupNode.RuleId ruleId = valueToRuleIds.get(val);
                    if (ruleId.getUpperValue().equalsIgnoreCase(val)) {
                        set.addAll((Collection<?>)ruleId.getRuleIds());
                    }
                }
                if (valueToRuleIds.containsKey("")) {
                    final LookupNode.RuleId ruleId = valueToRuleIds.get("");
                    if (ruleId.getUpperValue().equalsIgnoreCase("")) {
                        set.addAll((Collection<?>)ruleId.getRuleIds());
                    }
                }
                final NavigableMap<BigInteger, LookupNode.RuleId> integerToRulIDMap = new TreeMap<BigInteger, LookupNode.RuleId>();
                try {
                    final BigInteger minVal;
                    final Map<BigInteger, V> map;
                    valueToRuleIds.entrySet().forEach(entry -> {
                        minVal = new BigInteger(entry.getKey());
                        map.put(minVal, entry.getValue());
                        return;
                    });
                }
                catch (Exception e) {
                    ruleSets.add(set);
                    ++index;
                    continue;
                }
                final NavigableMap<BigInteger, LookupNode.RuleId> lessThanKey = integerToRulIDMap.headMap(new BigInteger(val), true);
                final String s;
                BigInteger valueInt;
                final AbstractCollection collection;
                lessThanKey.entrySet().forEach(entry -> {
                    try {
                        valueInt = new BigInteger(s);
                    }
                    catch (Exception e2) {
                        return;
                    }
                    if (entry != null) {
                        if (valueInt.compareTo(entry.getValue().getUpperValueInt()) != 1) {
                            collection.addAll(entry.getValue().getRuleIds());
                        }
                    }
                    return;
                });
                ruleSets.add(set);
                ++index;
            }
            String bestRuleId = "";
            String result = this.defaultResult;
            if (ruleSets.size() > 0) {
                final HashSet<String> intersection = ruleSets.get(0);
                for (int i = 0; i < ruleSets.size(); ++i) {
                    intersection.retainAll(ruleSets.get(i));
                }
                bestRuleId = this.getBestRuleId(intersection);
                final LookupNode.RangeRule bestrule = this.rules.get(bestRuleId);
                if (bestrule != null) {
                    result = bestrule.getResult();
                }
            }
            final Object[] rowfields = new Object[row.size() + 2];
            final Object[] rowfieldstemp = new Object[row.size() + 1];
            for (int j = 0; j < row.size(); ++j) {
                rowfields[j] = row.get(j);
                rowfieldstemp[j] = row.get(j);
            }
            rowfields[row.size()] = bestRuleId;
            rowfields[row.size() + 1] = result;
            rowfieldstemp[row.size()] = bestRuleId;
            return RowFactory.create(rowfields);
        }
        
        public Column getColumnFromExp(final SparkSession spark, final String expression) {
            return SparkUtils.getColumnFromExp(spark, expression);
        }
    }
}
