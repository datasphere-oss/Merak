package io.merak.etl.executor.spark.progress;

import io.merak.metadata.api.dao.run_details.*;
import org.apache.spark.status.*;
import io.merak.metadata.repo.impl.*;
import java.util.concurrent.*;
import io.merak.metadata.impl.dto.run_details.*;
import java.util.*;
import org.apache.spark.scheduler.*;
import org.slf4j.*;

public class PipelineJobProgressListener extends SparkListener
{
    private String jobId;
    private volatile Map<Integer, Integer> stageMap;
    private RunDetailsDAO<RunDetails> runDetailsDAO;
    private RunDetails runDetails;
    AppStatusListener l;
    private volatile int numCompletedTasks;
    private volatile int numFailedTasks;
    private volatile int numSkippedTasks;
    private volatile int numKilledTasks;
    private volatile int numTasks;
    private volatile int numCompletedStages;
    private volatile int numFailedStages;
    private volatile int numSkippedStages;
    private volatile int numKilledStages;
    private volatile int numStages;
    private volatile Map<Integer, StageMetrics> stageMetricsMap;
    private static Logger LOGGER;
    
    public PipelineJobProgressListener(final String jobId) {
        this.jobId = jobId;
        this.runDetailsDAO = (RunDetailsDAO<RunDetails>)MetadataAccessFactory.getRunDetailsDAO();
        (this.runDetails = new RunDetails()).setIwJobId(jobId);
        this.stageMap = new ConcurrentHashMap<Integer, Integer>();
        this.stageMetricsMap = new ConcurrentHashMap<Integer, StageMetrics>();
    }
    
    public void onStageCompleted(final SparkListenerStageCompleted stageCompleted) {
        super.onStageCompleted(stageCompleted);
        final Integer stageId = stageCompleted.stageInfo().stageId();
        final RunDetailsJobInfo job = this.runDetailsDAO.findJob(this.jobId, Integer.valueOf(this.stageMap.get(stageId)));
        for (final RunDetailsStageInfo stage : job.getStages()) {
            if (stage.getStageId().equals(stageId)) {
                final int stageNumTasks = stageCompleted.stageInfo().numTasks();
                if (stageCompleted.stageInfo().failureReason() != null) {
                    job.setNumFailedStages(Integer.valueOf(++this.numFailedStages));
                    stage.setNumFailedTasks(Integer.valueOf(stageNumTasks));
                }
                else if (stageCompleted.stageInfo().submissionTime().isDefined()) {
                    job.setNumCompletedStages(Integer.valueOf(++this.numCompletedStages));
                    stage.setNumCompletedTasks(Integer.valueOf(stageNumTasks));
                }
                else {
                    job.setNumSkippedStages(Integer.valueOf(++this.numSkippedStages));
                    stage.setNumSkippedTasks(Integer.valueOf(stageNumTasks));
                }
            }
        }
        this.runDetailsDAO.upsertJob(this.jobId, job);
    }
    
    public void onStageSubmitted(final SparkListenerStageSubmitted stageSubmitted) {
        super.onStageSubmitted(stageSubmitted);
        final RunDetailsStageInfo stage = new RunDetailsStageInfo();
        stage.setStageId(Integer.valueOf(stageSubmitted.stageInfo().stageId()));
    }
    
    public void onTaskStart(final SparkListenerTaskStart taskStart) {
        super.onTaskStart(taskStart);
    }
    
    public void onTaskGettingResult(final SparkListenerTaskGettingResult taskGettingResult) {
        super.onTaskGettingResult(taskGettingResult);
    }
    
    public void onTaskEnd(final SparkListenerTaskEnd taskEnd) {
        super.onTaskEnd(taskEnd);
        final int stageId = taskEnd.stageId();
        final int jobId = this.stageMap.get(stageId);
        final RunDetailsStageInfo stage = this.runDetailsDAO.findStage(this.jobId, Integer.valueOf(jobId), Integer.valueOf(stageId));
        if (taskEnd.taskInfo().failed()) {
            stage.setNumCompletedTasks(Integer.valueOf(++this.numCompletedTasks));
        }
        this.runDetailsDAO.upsertStage(this.jobId, Integer.valueOf(jobId), stage);
    }
    
    public void onJobStart(final SparkListenerJobStart jobStart) {
        super.onJobStart(jobStart);
        final Integer jobId = jobStart.jobId();
        final RunDetailsJobInfo job = new RunDetailsJobInfo();
        job.setId(jobId);
        job.setName(String.valueOf(jobStart.jobId()));
        job.setStartTime(Long.valueOf(System.currentTimeMillis()));
        job.setStatus(JobStatus.RUNNING.name());
        for (int numStages = jobStart.stageInfos().size(), i = 0; i < numStages; ++i) {
            final RunDetailsStageInfo stage = new RunDetailsStageInfo();
            final StageInfo stageInfo = (StageInfo)jobStart.stageInfos().apply(i);
            final String stageName = String.format("%d.%s", stageInfo.stageId(), stageInfo.name());
            final int numTasks = stageInfo.numTasks();
            stage.setStageId(Integer.valueOf(stageInfo.stageId()));
            stage.setStageName(stageName);
            stage.setNumTasks(Integer.valueOf(numTasks));
            job.getStages().add(stage);
            final Integer stageId = stageInfo.stageId();
            this.stageMap.put(stageId, job.getId());
            final StageMetrics stageMetrics = new StageMetrics();
            stageMetrics.numTasks = numTasks;
            this.stageMetricsMap.put(stageId, stageMetrics);
        }
        this.runDetailsDAO.upsertJob(this.jobId, job);
    }
    
    public void onJobEnd(final SparkListenerJobEnd jobEnd) {
        super.onJobEnd(jobEnd);
        final RunDetailsJobInfo job = new RunDetailsJobInfo();
        job.setId(Integer.valueOf(jobEnd.jobId()));
        job.setEndTime(Long.valueOf(System.currentTimeMillis()));
        job.setStatus(JobStatus.TERMINATED.name());
        final int numSkippedStages = 0;
        job.setNumSkippedStages(Integer.valueOf(numSkippedStages));
        this.runDetailsDAO.upsertJob(this.jobId, job);
    }
    
    public void onEnvironmentUpdate(final SparkListenerEnvironmentUpdate environmentUpdate) {
        super.onEnvironmentUpdate(environmentUpdate);
    }
    
    public void onBlockManagerAdded(final SparkListenerBlockManagerAdded blockManagerAdded) {
        super.onBlockManagerAdded(blockManagerAdded);
    }
    
    public void onBlockManagerRemoved(final SparkListenerBlockManagerRemoved blockManagerRemoved) {
        super.onBlockManagerRemoved(blockManagerRemoved);
    }
    
    public void onUnpersistRDD(final SparkListenerUnpersistRDD unpersistRDD) {
        super.onUnpersistRDD(unpersistRDD);
    }
    
    public void onApplicationStart(final SparkListenerApplicationStart applicationStart) {
        super.onApplicationStart(applicationStart);
    }
    
    public void onApplicationEnd(final SparkListenerApplicationEnd applicationEnd) {
        super.onApplicationEnd(applicationEnd);
    }
    
    public void onExecutorMetricsUpdate(final SparkListenerExecutorMetricsUpdate executorMetricsUpdate) {
        super.onExecutorMetricsUpdate(executorMetricsUpdate);
    }
    
    public void onExecutorAdded(final SparkListenerExecutorAdded executorAdded) {
        super.onExecutorAdded(executorAdded);
    }
    
    public void onExecutorRemoved(final SparkListenerExecutorRemoved executorRemoved) {
        super.onExecutorRemoved(executorRemoved);
    }
    
    public void onBlockUpdated(final SparkListenerBlockUpdated blockUpdated) {
        super.onBlockUpdated(blockUpdated);
    }
    
    public void onOtherEvent(final SparkListenerEvent event) {
        super.onOtherEvent(event);
    }
    
    static {
        PipelineJobProgressListener.LOGGER = LoggerFactory.getLogger((Class)PipelineJobProgressListener.class);
    }
    
    private class StageMetrics
    {
        volatile int numCompletedTasks;
        volatile int numFailedTasks;
        volatile int numSkippedTasks;
        volatile int numKilledTasks;
        volatile int numTasks;
    }
    
    private enum JobStatus
    {
        STARTED, 
        RUNNING, 
        SUCCESS, 
        TERMINATED;
    }
}
