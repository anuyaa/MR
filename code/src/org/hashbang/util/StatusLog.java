package org.hashbang.util;

import org.hashbang.jobtracker.JobTracker;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.Socket;

public class StatusLog {
    private String server;
    public static final String MAP_COMPLETED = "Map task completed";
    public static final String JOB_COMPLETED = "Job completed";

    public StatusLog(String server) {
        this.server = server;
    }

    public void log(String msg) {
        try {
            Socket socket = new Socket(server, JobTracker.JOB_STATUS_PORT);
            PrintWriter writer = new PrintWriter(socket.getOutputStream(), true);
            writer.println(msg);
            socket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}