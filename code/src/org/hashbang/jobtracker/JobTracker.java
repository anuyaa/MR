package org.hashbang.jobtracker;

import org.hashbang.mr.Configuration;
import org.hashbang.mr.Job;
import org.hashbang.mr.JobContext;
import org.hashbang.mr.MapContext;
import org.hashbang.tasktracker.TaskTracker;
import org.hashbang.util.AutoDiscoverQueue;
import org.hashbang.util.DynamicLoader;
import org.hashbang.util.StatusLog;

import java.io.*;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * @author alwin
 */
public class JobTracker {

    public static final int JT_LISTEN_PORT = 9090;
    public static final int JOB_STATUS_PORT = 9092;
    public static final String TASK_TRACKER_IDENTIFIER = "TASKTRACKER";
    public static final String JOB_CLIENT_IDENTIFIER = "JOBCLIENT";
    public static final String TASK_COMPLETE_IDENTIFIER = "TASK_COMPLETE";
    private List<TaskTrackerHandler> taskTrackerHandlers;
    private List<String> taskTrackerNodes;
    private String reducerIP;

    public JobTracker() {
        taskTrackerHandlers = new ArrayList<>();
        taskTrackerNodes = new ArrayList<>();
    }

    /**
     * Returns the list of task tracker handlers
     *
     * @return taskTrackerHandlers
     */
    public List<TaskTrackerHandler> getTaskTrackerHandlers() {
        return taskTrackerHandlers;
    }

    /**
     * Returns the list of task trackers
     *
     * @return taskTrackerNodes
     */
    public List<String> getTaskTrackerNodes() {
        return taskTrackerNodes;
    }

    /**
     * Returns the reducer's IP address
     *
     * @return reducerIP
     */
    public String getReducer() {
        return reducerIP;
    }

    /**
     * Sets the reducer's IP address
     *
     * @param reducerIP
     */
    public void setReducer(String reducerIP) {
        this.reducerIP = reducerIP;
    }

    /**
     * Returns true if reducer is already assigned
     *
     * @return true if reducer is already assigned, otherwise false
     */
    public boolean isReducerAvailable() {
        return reducerIP != null;
    }

    /**
     * Returns a list of {@link MapContext} given a job and a jobId
     *
     * @param job
     * @param jobId
     * @return
     */
    public static List<MapContext> getContextsWithSplitInfo(Job job, int jobId) {
        List<MapContext> contexts = new ArrayList<>();
        File inputFile = new File(job.getInputPath());
        long fileLength = inputFile.length();
        Configuration conf = job.getConf();
        int blockSize = (int) Long.parseLong(conf.get("io.file.blocksize"))
                * 1024 * 1024;
        int numSplits = (int) Math.ceil(fileLength / (double) blockSize);
        long offset;
        int i;
        // iterating only numSplits-1 times to account for the
        // last split size not being equal to block size
        for (i = 0; i < (numSplits - 1); i++) {
            offset = blockSize * i;
            contexts.add(new MapContext(
                    jobId, job, offset, blockSize));
        }
        // handling last split size
        offset = blockSize * i;
        contexts.add(new MapContext(jobId, job, offset, (int) (fileLength - offset)));
        return contexts;
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        JobTracker jobTracker = new JobTracker();
        JTListener listener = new JTListener(jobTracker);
        listener.start();
    }
}

/**
 * @author alwin
 */
class JTListener extends Thread {
    JobTracker jobTracker;
    List<TaskTrackerHandler> taskTrackerHandlers;

    public JTListener(JobTracker jobTracker) {
        this.jobTracker = jobTracker;
        this.taskTrackerHandlers = jobTracker.getTaskTrackerHandlers();
    }

    public void run() {
        TaskTrackerHandler taskTrackerHandler;
        ServerSocket listener;
        try {
            listener = new ServerSocket(JobTracker.JT_LISTEN_PORT);
            //Clearing the SQS for a fresh start.
            AutoDiscoverQueue.clearQ();
            AutoDiscoverQueue.sendInfo(InetAddress.getLocalHost().getHostAddress());
            try {
                while (true) {
                    Socket socket = listener.accept();
                    BufferedReader in = new BufferedReader(
                            new InputStreamReader(socket.getInputStream()));
                    // check handshake info and decide which handler to call
                    String identifier = in.readLine();
                    if (identifier.contentEquals(JobTracker.JOB_CLIENT_IDENTIFIER)) {
                        synchronized (jobTracker) {
                            // reset reducer for each job
                            jobTracker.setReducer(null);
                        }
                        // start a job handler
                        new JobClientHandler(socket, jobTracker).start();
                    } else if (identifier.contentEquals(JobTracker.TASK_TRACKER_IDENTIFIER)) {
                        // send JobTracker's IP address to Amazon SQS as soon as a task tracker is connected
                        AutoDiscoverQueue.sendInfo(InetAddress.getLocalHost().getHostAddress());
                        taskTrackerHandler = new TaskTrackerHandler(socket, jobTracker);
                        taskTrackerHandlers.add(taskTrackerHandler);
                        taskTrackerHandler.start();
                    } else if (identifier.contentEquals(JobTracker.TASK_COMPLETE_IDENTIFIER)) {
                        announceReducer(socket);
                    }
                }
            } finally {
                listener.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Sets the reducerIP field if its not already set and sends the reducerIP
     * to the given socket
     *
     * @param socket
     */
    private void announceReducer(Socket socket) {
        synchronized (jobTracker) {
            if (!jobTracker.isReducerAvailable()) {
                jobTracker.setReducer(socket.getInetAddress().getHostAddress());
            }
        }
        try {
            PrintWriter out =
                    new PrintWriter(socket.getOutputStream(), true);
            out.println(jobTracker.getReducer());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

/**
 * @author alwin
 */
class JobClientHandler extends Thread {

    private static int jobCounter = 1;
    private Socket socket;
    private JobTracker jobTracker;
    private JobStatusUpdater jobStatusUpdater;

    public JobClientHandler(Socket socket, JobTracker jobTracker) {
        this.socket = socket;
        this.jobTracker = jobTracker;
        jobStatusUpdater = new JobStatusUpdater(socket);
        jobStatusUpdater.start();
    }

    public void run() {
        try {
            // load the user job jar dynamically
            DynamicLoader.loadClass(JobContext.JOB_JAR_FILE);
            // read Job object from user
            ObjectInputStream objectInputStream = new ObjectInputStream(socket.getInputStream());
            Job clientJob = (Job) objectInputStream.readObject();
            List<TaskTrackerHandler> taskTrackerHandlers = jobTracker.getTaskTrackerHandlers();
            int numHandlers = taskTrackerHandlers.size();
            int taskCount[] = new int[numHandlers];
            String logMsg = "Task Trackers available: " + numHandlers
                    + "\nRunning job : ID " + jobCounter
                    + "\nDividing job among task trackers";
            StatusLog statusLog = new StatusLog(InetAddress.getLoopbackAddress().getHostAddress());
            // send an update to the user via JobStatusUpdater
            statusLog.log(logMsg);
            // decide on the different offsets for the task trackers
            // and send the context with this info
            List<MapContext> contexts = JobTracker.getContextsWithSplitInfo(clientJob, jobCounter++);

            // set number of required/active TTs
            int numActiveTTs = Math.min(numHandlers, contexts.size());
            for (int i = 0; i < contexts.size(); ) {
                for (int j = 0; j < numHandlers; j++, i++) {
                    if (i == contexts.size()) {
                        break;
                    }
                    // count number of tasks for each TT
                    taskCount[j]++;
                }
            }

            statusLog.log("Starting map task");
            for (int i = 0; i < contexts.size(); ) {
                for (int j = 0; j < numHandlers; j++) {
                    if (i == contexts.size())
                        break;
                    // set number of active TTs in each context
                    contexts.get(i).setNumActiveTTs(numActiveTTs);
                    taskTrackerHandlers.get(j).submitTask(contexts.get(i++), taskCount[j]);
                }
            }
            // clear the task tracker handlers
            taskTrackerHandlers.clear();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}

/**
 * @author alwin
 */
class JobStatusUpdater extends Thread {
    private PrintWriter writer;

    public JobStatusUpdater(Socket socket) {
        try {
            this.writer = new PrintWriter(socket.getOutputStream(), true);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void run() {
        try {
            ServerSocket listener = new ServerSocket(JobTracker.JOB_STATUS_PORT);
            BufferedReader br;
            String line;
            String timeStamp;
            int mapCount = 0;
            boolean running = true;
            long startTime = 0;
            long elapsedTime;
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            while (running) {
                Socket socket = listener.accept();
                br = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                while ((line = br.readLine()) != null) {
                    // save start time if not already set
                    if (startTime == 0) {
                        startTime = System.currentTimeMillis();
                    }
                    timeStamp = dateFormat.format(new Date());
                    if (line.contains(StatusLog.MAP_COMPLETED)) {
                        mapCount++;
                        line += " - " + mapCount;
                    } else if (line.contains((StatusLog.JOB_COMPLETED))) {
                        // save elapsed time after job completion
                        running = false;
                        elapsedTime = System.currentTimeMillis() - startTime;
                        line += ". Total execution time (in seconds): " + (elapsedTime / 1000.0);
                    }
                    // send the message to the client
                    writer.println(timeStamp + " -- " + line);
                }
            }
            listener.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

/**
 * @author alwin
 */
class TaskTrackerHandler extends Thread {

    private Socket socket;
    private final JobTracker jobTracker;
    private String taskTrackerIP;
    private Socket ttSocket;

    public TaskTrackerHandler(Socket socket, JobTracker jobTracker) throws IOException {
        this.socket = socket;
        this.taskTrackerIP = socket.getInetAddress().getHostAddress();
        this.jobTracker = jobTracker;
    }

    /**
     * Connects to the task tracker and sends the job context and the total
     * number of tasks assigned to it
     *
     * @param context
     * @param numTasks
     */
    public void submitTask(JobContext context, int numTasks) {
        try {
            // create a new socket connection to the task tracker
            ttSocket = new Socket(taskTrackerIP, TaskTracker.TT_LISTEN_PORT);
            ObjectOutputStream out = new ObjectOutputStream(ttSocket.getOutputStream());
            out.writeInt(numTasks);
            // write JobContext object to socket
            out.writeObject(context);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void run() {
        synchronized (jobTracker) {
            // get remote IP address of task tracker from socket
            jobTracker.getTaskTrackerNodes().add(socket.getInetAddress().getHostAddress());
        }
        System.out.println("Task Tracker node " + socket.getInetAddress().getHostAddress()
                + " is up and ready!");
    }
}