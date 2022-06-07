package io.merak.etl.executor.spark.executor;

import org.apache.spark.sql.*;
import com.google.common.collect.*;
import java.util.stream.*;

import scala.collection.*;
import io.merak.adapters.filesystem.*;
import java.util.*;
import java.io.*;
import io.merak.etl.executor.impl.*;
import io.merak.etl.sdk.pipeline.*;
import io.merak.etl.executor.spark.metadata.*;
import io.merak.etl.executor.spark.translator.dto.*;
import io.merak.etl.executor.spark.utils.*;
import merak.tools.ExceptionHandling.*;

import org.apache.spark.sql.types.*;
import io.merak.etl.pipeline.dto.*;
import org.slf4j.*;

public class SparkHelper
{
    protected static final String PATH_OPTION = "path";
    protected static final Logger LOGGER;
    
    public static DataFrameWriter getMergeTableWriter(final TargetNode targetNode, final Dataset<Row> dataset, final String targetLoc) {
        DataFrameWriter<Row> dfWriter = (DataFrameWriter<Row>)dataset.coalesce(1).write();
        List<String> buckets = (List<String>)Lists.newArrayList();
        if (targetNode.secondaryColumnsExist()) {
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
            dfWriter = (DataFrameWriter<Row>)dfWriter.bucketBy(numBuckets, firstBucketCol, (Seq)SparkUtils.getStringSeqFromList(bucketColumns));
        }
        dfWriter = (DataFrameWriter<Row>)dfWriter.format(targetNode.getStorageFormat()).option("path", targetLoc);
        return dfWriter;
    }
    
    public static Set<String> getBucketPatterns(final String parentPath, final IWFileSystem fs) throws IOException {
        final Location path = fs.createLocation(parentPath);
        SparkHelper.LOGGER.trace("Computing bucket patterns for: {}", (Object)parentPath);
        final HdfsUtils hdfsUtils = fs.getHdfsUtils();
        final List<String> filesRecursive = (List<String>)hdfsUtils.getFilesRecursive(path.toString());
        final List<String> partitionList = new ArrayList<String>();
        for (final String i : filesRecursive) {
            final String[] segments = i.split("_");
            final String pattern = "*_" + segments[1];
            SparkHelper.LOGGER.trace("Found bucket Pattern: {}", (Object)pattern);
            partitionList.add(pattern);
        }
        final Set<String> partitionSet = new HashSet<String>(partitionList);
        return partitionSet;
    }
    
    public static int getBucketNumber(final String bucketPattern) {
        SparkHelper.LOGGER.debug("Getting bucket number for {}", (Object)bucketPattern);
        final String[] pattern = bucketPattern.substring(bucketPattern.indexOf("_") + 1).split("\\.");
        final int number = Integer.parseInt(pattern[0]);
        return number;
    }
    
    public static void deleteUpdatebucket(final Set<String> updatedBuckets, final String parentPath, final IWFileSystem fs) throws IOException {
        SparkHelper.LOGGER.debug("Deleting updated buckets from from main table location :");
        final Location path = fs.createLocation(parentPath);
        final HdfsUtils hdfsUtils = fs.getHdfsUtils();
        final List<String> filesRecursive = (List<String>)hdfsUtils.getRecursiveFilesAbsolutePath(path.toString());
        for (final String i : filesRecursive) {
            final String[] paths = i.split("/");
            final String fileName = paths[paths.length - 1];
            final String[] segments = fileName.split("_");
            final String pattern = "*_" + segments[1];
            if (updatedBuckets.contains(pattern)) {
                hdfsUtils.deleteDirIfExists(i);
            }
            SparkHelper.LOGGER.trace("Successfully deleted updated bucket: {}", (Object)i);
        }
    }
    
    public static void moveMergedBucket(final String sourcePath, final String destPath, final IWFileSystem fs) throws IOException {
        SparkHelper.LOGGER.debug("Moving merged files to main table location :");
        SparkHelper.LOGGER.debug("Form {} to {}", (Object)sourcePath, (Object)destPath);
        final Location source = fs.createLocation(sourcePath);
        final Location dest = fs.createLocation(destPath);
        final HdfsUtils hdfsUtils = fs.getHdfsUtils();
        final List<String> filesRecursive = (List<String>)hdfsUtils.getRecursiveFilesAbsolutePath(source.toString());
        for (final String i : filesRecursive) {
            SparkHelper.LOGGER.debug("Moving merged file {}", (Object)i);
            hdfsUtils.moveFileToFile(i, dest.getFullPath());
        }
        SparkHelper.LOGGER.trace("Successfully moved all the files");
    }
    
    public static void moveDeltaBucket(final String sourcePath, final String destPath, final Set<String> newBuckets, final IWFileSystem fs) throws IOException {
        SparkHelper.LOGGER.debug("Moving delta files to main table location :");
        SparkHelper.LOGGER.debug("Form {} to {}", (Object)sourcePath, (Object)destPath);
        final Location path = fs.createLocation(sourcePath);
        final Location dest = fs.createLocation(destPath);
        final HdfsUtils hdfsUtils = fs.getHdfsUtils();
        final List<String> absoluteFilesRecursive = (List<String>)hdfsUtils.getRecursiveFilesAbsolutePath(path.toString());
        for (final String i : absoluteFilesRecursive) {
            final String[] paths = i.split("/");
            final String fileName = paths[paths.length - 1];
            final String[] segments = fileName.split("_");
            final String pattern = "*_" + segments[1];
            if (newBuckets.contains(pattern)) {
                hdfsUtils.moveFileToFile(i, dest.getFullPath());
            }
        }
        SparkHelper.LOGGER.trace("Successfully moved all the files");
    }
    
    public static String cleanUrl(String path) {
        path = path.replaceAll("//", "/");
        return path;
    }
    
    public static ExecutionResult generateResult(final SparkTranslatorState sparkTranslatorState, final PipelineNode node, final int limit) {
        if (!(node instanceof EndVertex)) {
            SparkHelper.LOGGER.error("{} is not a valid sink", (Object)node.getId());
            throw new IWException(String.format("{} is not a valid sink", node.getId()));
        }
        final DataFrameObject dataFrame = sparkTranslatorState.getDataFrame(node);
        final Dataset<Row> dataset = dataFrame.getDataset();
        final Row[] rows = (Row[])dataset.take(limit);
        SparkHelper.LOGGER.info("count {} limit {}", (Object)rows.length, (Object)limit);
        final StructType schema = dataset.schema();
        final ExecutionResult result = new ExecutionResult((EndVertex)node);
        SparkMetaDataUtils.populate(rows, schema, result);
        return result;
    }
    
    static {
        LOGGER = LoggerFactory.getLogger((Class)SparkHelper.class);
    }
}
