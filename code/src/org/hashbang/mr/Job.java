package org.hashbang.mr;

import org.hashbang.jobtracker.JobTracker;
import org.hashbang.util.StatusLog;

import java.io.*;
import java.net.InetAddress;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;


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
 * job via {@link Job} and then submits the job and monitor its progress.</p>
 * <p/>
 * <p>Here is an example on how to submit a job:</p>
 * <p><blockquote><pre>
 *     // Create a new Job
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

public class Job implements Context, Serializable {

    protected String jobName;
    protected long startTime;
    protected long finishTime;
    protected int numReduceTasks;
    protected Class<? extends Mapper> mapperClass;
    protected Class<? extends Reducer> reducerClass;
    protected String inputPath;
    protected String outputDir;
    protected Configuration conf;

    public Job() {
        this(new Configuration());
    }

    public Job(Configuration conf) {
        this.numReduceTasks = 1;
        this.conf = conf;
    }

    /**
     * Get start time of the job.
     *
     * @return the start time of the job
     */
    public long getStartTime() {
        return startTime;
    }

    /**
     * Get finish time of the job.
     *
     * @return the finish time of the job
     */
    public long getFinishTime() {
        return finishTime;
    }

    /**
     * The user-specified job name.
     */
    public String getJobName() {
        return jobName;
    }

    /**
     * Set the user-specified job name.
     *
     * @param name the job's new name.
     */
    public void setJobName(String name) {
        this.jobName = name;
    }

    public int getNumReduceTasks() {
        return numReduceTasks;
    }

    /**
     * Set the number of reduce tasks for the job.
     *
     * @param tasks the number of reduce tasks
     */
    public void setNumReduceTasks(int tasks) {

    }

    /**
     * Get the {@link Mapper} class for the job.
     *
     * @return the {@link Mapper} class for the job.
     */
    public Class<? extends Mapper> getMapperClass() {
        return mapperClass;
    }

    /**
     * Set the {@link Mapper} for the job.
     *
     * @param cls the <code>Mapper</code> to use
     */
    public void setMapperClass(Class<? extends Mapper> cls) {
        this.mapperClass = cls;
    }

    /**
     * Get the {@link Reducer} class for the job.
     *
     * @return the {@link Reducer} class for the job.
     */
    public Class<? extends Reducer> getReducerClass() {
        return reducerClass;
    }

    /**
     * Set the {@link Reducer} for the job.
     *
     * @param cls the <code>Reducer</code> to use
     */
    public void setReducerClass(Class<? extends Reducer> cls) {
        this.reducerClass = cls;
    }

    /**
     * Get the InputPath for the Job
     *
     * @return inputPath
     */
    public String getInputPath() {
        return inputPath;
    }

    /**
     * Get the OutputDir for the Job
     * @return outputDir
     */
    public String getOutputDir() {
        return outputDir;
    }

    /**
     * Get the Configuration for the Job
     * @return conf
     */
    public Configuration getConf() {
        return conf;
    }

    /**
     * Add a {@link Path} as input for the map-reduce job.
     *
     * @param path {@link Path} to be added as the input for
     *             the map-reduce job.
     */
    public void addInputPath(String path) {
        this.inputPath = path;
    }

    /**
     * Set the {@link Path} of the output directory for the map-reduce job.
     *
     * @param outputDir the {@link Path} of the output directory for
     *                  the map-reduce job.
     */
    public void setOutputPath(String outputDir) {
        this.outputDir = outputDir;
    }

    /**
     * Submits the job to {@link JobTracker} and waits for job completion
     */
    public void waitForCompletion() {
        checkIfOutputDirExists();
        try {
            Socket socket = new Socket(InetAddress.getLocalHost().getHostAddress(),
                    JobTracker.JT_LISTEN_PORT);
            PrintWriter w = new PrintWriter(socket.getOutputStream(), true);
            w.println(JobTracker.JOB_CLIENT_IDENTIFIER);
            ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
            out.writeObject(this);
            // wait for completion by reading an integer from JT
            String line;
            BufferedReader br = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            while ((line = br.readLine()) != null) {
                System.out.println(line);
                if (line.contains(StatusLog.JOB_COMPLETED)) {
                    break;
                }
            }
            socket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Check if the Job output dir exists.
     */
    private void checkIfOutputDirExists() {
        if (Files.exists(Paths.get(outputDir))) {
            System.out.println("Error: Please remove the output directory '"
                    + outputDir + "' and try again.");
            System.exit(1);
        }
    }
}

