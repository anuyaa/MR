package org.hashbang.jobtracker;

/**
 *
 */
public interface IJobTracker {

    /**
     * Parses the commandline arguments
     */
    public void parseArgs(String[] args);

    /**
     * Creates a {@link org.hashbang.mr.JobContext} for the submitted job
     */
    public void createJobContext();

    /**
     * Submits map or reduce tasks to the task trackers.
     */
    public void submitTask();

}
