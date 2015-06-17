package org.hashbang.mr;

import java.nio.ByteBuffer;

/**
 * A view of the job that is provided to the tasks while they
 * are running.
 */
public class JobContext extends Job implements Context {

    protected long jobId;
    public static final String JOB_BASE_DIR = System.getenv("HASHBANG_TMP_DIR");
    public static final String MAP_OUTPUT_FILE_PREFIX = "__HB__";
    public static final String JOB_JAR_FILE = System.getenv("HASHBANG_HOME") + "/hashbang_job.jar";
    protected String workingDir;
    protected String namenodeServerIP;
    protected int dataSize;
    protected boolean isJobCompleted;
    protected int numActiveTTs;

    protected long splitOffset;
    protected ByteBuffer dataBuffer;

    public JobContext(int jobId, Job job, long offset, int dataSize) {
        this.jobId = jobId;
        this.splitOffset = offset;
        this.dataSize = dataSize;
        this.jobName = job.getJobName();
        this.startTime = job.getStartTime();
        this.finishTime = job.getFinishTime();
        this.numReduceTasks = job.getNumReduceTasks();
        this.mapperClass = job.getMapperClass();
        this.reducerClass = job.getReducerClass();
        this.inputPath = job.getInputPath();
        this.outputDir = job.getOutputDir();
        this.isJobCompleted = false;
    }

    public JobContext(JobContext context) {
        this.splitOffset = context.getSplitOffset();
        this.dataSize = context.getDataSize();
        this.jobName = context.getJobName();
        this.startTime = context.getStartTime();
        this.finishTime = context.getFinishTime();
        this.numReduceTasks = context.getNumReduceTasks();
        this.mapperClass = context.getMapperClass();
        this.reducerClass = context.getReducerClass();
        this.inputPath = context.getInputPath();
        this.outputDir = context.getOutputDir();
        this.jobId = context.getJobID();
        this.isJobCompleted = context.isJobCompleted();
        this.namenodeServerIP = context.getNamenodeServerIP();
        this.workingDir = context.getWorkingDirectory();
        this.numActiveTTs = context.getNumActiveTTs();
    }

    public long getSplitOffset() {
        return splitOffset;
    }

    public int getDataSize() {
        return dataSize;
    }

    public int getNumActiveTTs() {
        return numActiveTTs;
    }

    public void setNumActiveTTs(int numActiveTTs) {
        this.numActiveTTs = numActiveTTs;
    }

    public void setNamenodeServerIP(String serverIP) {
        this.namenodeServerIP = serverIP;
    }

    public String getNamenodeServerIP() {
        return namenodeServerIP;
    }

    public ByteBuffer getDataBuffer() {
        return dataBuffer;
    }

    public void setDataBuffer(ByteBuffer dataBuffer) {
        this.dataBuffer = dataBuffer;
    }

    /**
     * Get the unique ID for the job.
     *

     * @return the job id
     */
    public long getJobID() {
        return jobId;
    }

    /**
     * Get the current working directory for the default file system.
     *
     * @return the directory name.
     */
    public String getWorkingDirectory() {
        return this.workingDir;
    }

    /**
     * @param workingDir
     */
    public void setWorkingDir(String workingDir) {
        this.workingDir = workingDir;
    }

    public void setIsJobCompleted(boolean b) {
        this.isJobCompleted = b;
    }

    public boolean isJobCompleted() {
        return isJobCompleted;
    }
}
