package org.hashbang.tasktracker;

import org.hashbang.fs.NamenodeClient;
import org.hashbang.jobtracker.JobTracker;
import org.hashbang.util.AutoDiscoverQueue;
import org.hashbang.mr.JobContext;
import org.hashbang.mr.MapContext;
import org.hashbang.mr.ReduceContext;
import org.hashbang.util.DynamicLoader;
import org.hashbang.util.StatusLog;

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.*;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

/**
 * Created by Navin
 */
public class TaskTracker {

    public static final int POLL_SQS_TIME_INTR = 60000; //in milli seconds
    public static final int TT_LISTEN_PORT = 9091;
    private String jobTrackerIP;
    private Queue<JobContext> jobQueue;

    public TaskTracker() {
        jobQueue = new LinkedList<JobContext>();
        ttAnnounce();
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        while (true) {
            TaskTracker taskTracker = new TaskTracker();
            TaskScheduler scheduler = new TaskScheduler(taskTracker);
            scheduler.start();
            TTListener ttListener = new TTListener(taskTracker);
            ttListener.start();
            synchronized (ttListener) {
                // wait till all current tasks complete
                ttListener.wait();
            }
            scheduler.terminate();
        }
    }

    public String getJobTrackerIP() {
        return jobTrackerIP;
    }

    public Queue<JobContext> getJobQueue() {
        return jobQueue;
    }

    /**
     * Fetches the JobTracker IP from the SQS and announces itself to the JobTracker.
     */
    public void ttAnnounce() {

        jobTrackerIP = AutoDiscoverQueue.getInfo();
        while (null == jobTrackerIP) {
            jobTrackerIP = AutoDiscoverQueue.getInfo();
            try {
                Thread.sleep(POLL_SQS_TIME_INTR);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        try {
            Socket socket = new Socket(jobTrackerIP, JobTracker.JT_LISTEN_PORT);
            PrintWriter out =
                    new PrintWriter(socket.getOutputStream(), true);
            out.println(JobTracker.TASK_TRACKER_IDENTIFIER);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

/**
 * TTListener: Listens to incoming map tasks from the JobTracker.
 */
class TTListener extends Thread {

    List<TaskHandler> taskHandlers;
    TaskTracker taskTracker;
    ServerSocket listener;

    public TTListener(TaskTracker taskTracker) {
        this.taskTracker = taskTracker;
        taskHandlers = new ArrayList<TaskHandler>();
    }

    public void run() {
        TaskHandler taskHandler;
        int numTasks; // Number of tasks
        int counter = 0;
        JobContext context;

        try {
            listener = new ServerSocket(TaskTracker.TT_LISTEN_PORT);
            try {
                do {
                    Socket socket = listener.accept();
                    ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
                    numTasks = ois.readInt();
                    taskHandler = new TaskHandler(ois, taskTracker);
                    taskHandlers.add(taskHandler);
                    taskHandler.start();
                    counter++;
                    System.out.println("added " + counter + " out of " + numTasks + " tasks to handlers");
                } while (counter < numTasks);

                // Waiting for task handlers join
                int i;
                TaskHandler th;
                for (i = 0; i < taskHandlers.size() - 1; i++) {
                    th = taskHandlers.get(i);
                    th.join();
                    System.out.println((i + 1) + " out of " + taskHandlers.size() + " tasks completed");
                }
                th = taskHandlers.get(i);
                th.join();
                System.out.println("All tasks completed");
                // get context info from the last task handler
                context = th.getJobContext();
                // All taskhandlers joined

                listener.close();
                StatusLog statusLog = new StatusLog(taskTracker.getJobTrackerIP());
                statusLog.log(StatusLog.MAP_COMPLETED);
                // notify JT, gext reducer IP and start reduce task
                startReduceTask(getReducerFromJT(), new ReduceContext(context));
            } catch (InterruptedException e) {
                e.printStackTrace();
            } finally {
                synchronized (this) {
                    // notify TaskTracker.main() that all tasks are complete
                    notify();
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    /**
     * Start the reduce task.
     * There are 2 cases:
     * 1) The current task tracker becomes the reducer, if it the first one to finish
     * all the map tasks. It then waits for all the other mappers to transfer the intermediate map outputs
     * 2) In the second case, it just sends the map outputs to the reducer node.
     *
     * @param reducerIP
     * @param context
     * @throws IOException
     */
    private void startReduceTask(String reducerIP, ReduceContext context) throws IOException {
        if (InetAddress.getLocalHost().getHostAddress().contentEquals(reducerIP)) {
            // if this node is the reducer
            // start a reducer listener to accept file transfer completion notification from other mappers
            ReducerListener reducerListener = new ReducerListener(context);
            reducerListener.start();
            System.out.println("Initiating reducer on this node");
            try {
                reducerListener.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            StatusLog statusLog = new StatusLog(taskTracker.getJobTrackerIP());
            statusLog.log("Completed map task(s)\nStarting reduce task");
            TaskScheduler.runTask(context);
            statusLog.log(StatusLog.JOB_COMPLETED);
        } else {
            Socket socket = new Socket(reducerIP, TaskTracker.TT_LISTEN_PORT);
            // sending map output using scp
            String srcDir = context.getWorkingDirectory() + "/" + JobContext.MAP_OUTPUT_FILE_PREFIX + "* ";
            String destDir = context.getWorkingDirectory() + "/";
            String[] cmd = new String[]{"/bin/bash", "-c", "scp -C -q -oStrictHostKeyChecking=no -oUserKnownHostsFile=/dev/null "
                    + srcDir + " " + reducerIP + ":" + destDir};
            Process p = Runtime.getRuntime().exec(cmd);
            try {
                p.waitFor();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            // signalling the listener that file copying is done by write 1 to its stream
            socket.getOutputStream().write(1);
            System.out.println("Sent map output to " + reducerIP);
        }
    }

    /**
     * Returns the IP of the reducer to the caller.
     *
     * @return reducer
     */
    private String getReducerFromJT() {
        String jobTrackerIP = taskTracker.getJobTrackerIP();
        String reducer = null;
        try {
            Socket socket = new Socket(jobTrackerIP, JobTracker.JT_LISTEN_PORT);
            PrintWriter out =
                    new PrintWriter(socket.getOutputStream(), true);
            out.println(JobTracker.TASK_COMPLETE_IDENTIFIER);
            BufferedReader in = new BufferedReader(
                    new InputStreamReader(socket.getInputStream()));
            reducer = in.readLine();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return reducer;
    }
}

/**
 * TaskHandler :
 * Spawned for each task and waits for the task scheduler to finish the current jobContext.
 */
class TaskHandler extends Thread {

    private ObjectInputStream ois;
    //private Socket socket;
    private TaskTracker taskTracker;
    private JobContext jobContext;

    public TaskHandler(ObjectInputStream ois, TaskTracker taskTracker) throws IOException {
        this.ois = ois;
        //this.socket = socket;
        this.taskTracker = taskTracker;
    }

    public void run() {
        try {
            // load job jar dynamically
            DynamicLoader.loadClass(JobContext.JOB_JAR_FILE);
            // Waiting for JobTracker to give a job
            jobContext = (JobContext) ois.readObject();
            jobContext.setNamenodeServerIP(taskTracker.getJobTrackerIP());
            synchronized (taskTracker) {
                taskTracker.getJobQueue().add(jobContext);
            }
            //Waiting for the task to finish.
            boolean isCompleted = false;
            while (!isCompleted) {
                synchronized (jobContext) {
                    isCompleted = jobContext.isJobCompleted();
                }
            }
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public JobContext getJobContext() {
        return jobContext;
    }
}

/**
 * TaskScheduler:
 * Spawned when the task tracker starts. It constantly polls the Job queue for jobcontext and calls runTask on each.
 */
class TaskScheduler extends Thread {
    private TaskTracker taskTracker;
    private boolean running;

    public TaskScheduler(TaskTracker taskTracker) {
        this.taskTracker = taskTracker;
        this.running = true;
    }

    /**
     * Used to stop the TaskScheduler to prepare for a new Job.
     */
    public void terminate() {
        running = false;
    }

    public void run() {
        JobContext context;
        NamenodeClient client;
        while (running) {
            synchronized (taskTracker) {
                context = taskTracker.getJobQueue().poll();
            }
            if (context != null) {
                System.out.println("Running task");
                String fileName = context.getInputPath();
                long splitOffset = context.getSplitOffset();
                int dataSize = context.getDataSize();

                client = new NamenodeClient(context.getNamenodeServerIP());

                byte[] dataBuffer = client.getData(fileName, splitOffset, dataSize);
                context.setDataBuffer(ByteBuffer.wrap(dataBuffer));
                long jobID = context.getJobID();
                try {
                    context.setWorkingDir(JobContext.JOB_BASE_DIR + "/" + jobID);
                    Path path = Paths.get(context.getWorkingDirectory());
                    if (!Files.exists(path)) {
                        Files.createDirectory(path);
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
                runTask(context);
                synchronized (context) {
                    context.setIsJobCompleted(true);
                }
            } else {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * This function is called for executing either a map task or reduce task.
     * @param context
     */
    public static void runTask(JobContext context) {
        try {
            Class cls = null;

            Method runMethod = null;
            if (context instanceof MapContext) {
                cls = context.getMapperClass();
                runMethod = cls.getMethod("run", MapContext.class);
            } else if (context instanceof ReduceContext) {
                cls = context.getReducerClass();
                runMethod = cls.getMethod("run", ReduceContext.class);
            }
            // invoke the run method of Mapper or Reducer and pass JobContext object
            // as parameter
            if (runMethod != null) {
                runMethod.invoke(cls.newInstance(), context);
            }
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (NoSuchMethodException e) {
            e.printStackTrace();
        } catch (InvocationTargetException e) {
            e.printStackTrace();
        }
    }
}

/**
 * ReduceListener:
 * Start a reducer listener to accept file transfer completion notification from other mappers
 *
 */
class ReducerListener extends Thread {

    private ServerSocket serverSocket;
    private ReduceContext context;

    public ReducerListener(ReduceContext context) {
        this.context = context;
    }

    @Override
    public void run() {
        try {
            serverSocket = new ServerSocket(TaskTracker.TT_LISTEN_PORT);
            int count = 0;
            while (count < context.getNumActiveTTs() - 1) {
                Socket socket = serverSocket.accept();
                // reads the 1 written by other mappers
                socket.getInputStream().read();
                count++;
            }
            // all mappers data has been received
        } catch (IOException exc) {
            exc.printStackTrace();
        } finally {
            try {
                serverSocket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }
}