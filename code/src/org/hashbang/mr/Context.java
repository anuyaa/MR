package org.hashbang.mr;

import java.io.IOException;
import java.nio.file.Path;

/**
 * The job submitter's view of the Job.
 * <p/>
 * <p>It allows the user to configure the
 * job, submit it, control its execution, and query the state. The set methods
 * only work until the job is submitted, afterwards they will throw an
 * IllegalStateException. </p>
 * <p/>
 * <p>
 * Normally the user creates the application, describes various facets of the
 * job via {@link Context} and then submits the job and monitor its progress.</p>
 * <p/>
 * <p>Here is an example on how to submit a job:</p>
 * <p><blockquote><pre>
 *     // Create a new Job
 *     Job job = Job.getInstance();
 *     job.setJarByClass(MyJob.class);
 * <p/>
 *     // Specify various job-specific parameters
 *     job.setJobName("myjob");
 * <p/>
 *     job.setInputPath(new Path("in"));
 *     job.setOutputPath(new Path("out"));
 * <p/>
 *     job.setMapperClass(MyJob.MyMapper.class);
 *     job.setReducerClass(MyJob.MyReducer.class);
 * </pre></blockquote></p>
 */

public interface Context {

    /**
     * The user-specified job name.
     */
    public String getJobName();

    /**
     * Set the {@link Mapper} for the job.
     *
     * @param cls the <code>Mapper</code> to use
     * @throws IllegalStateException if the job is submitted
     */
    public void setMapperClass(Class<? extends Mapper> cls) throws IllegalStateException;

    /**
     * Set the {@link Reducer} for the job.
     *
     * @param cls the <code>Reducer</code> to use
     * @throws IllegalStateException if the job is submitted
     */
    public void setReducerClass(Class<? extends Reducer> cls) throws IllegalStateException;

    /**
     * Set the user-specified job name.
     *
     * @param name the job's new name.
     * @throws IllegalStateException if the job is submitted
     */
    public void setJobName(String name) throws IllegalStateException;

    /**
     * Add a {@link Path} as input for the map-reduce job.
     *
     * @param path {@link Path} to be added as the input for
     *            the map-reduce job.
     * @throws IOException if input file does not exist
     */
    public void addInputPath(String path) throws IOException;

    /**
     * Set the {@link Path} of the output directory for the map-reduce job.
     *
     * @param outputDir the {@link Path} of the output directory for
     * the map-reduce job.
     */
    public void setOutputPath(String outputDir);

    public Class<? extends Mapper> getMapperClass();

    /**
     * Get the {@link Reducer} class for the job.
     *
     * @return the {@link Reducer} class for the job.
     */
    public Class<? extends Reducer> getReducerClass();

}




