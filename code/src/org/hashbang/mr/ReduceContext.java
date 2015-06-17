package org.hashbang.mr;

import org.hashbang.fs.NamenodeService;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.Socket;

/**
 * A view of the job that is provided to the tasks while they
 * are running.
 */

public class ReduceContext <KEYOUT, VALUEOUT> extends JobContext {

    private Socket socket;
    private PrintWriter writer;

    public ReduceContext(JobContext context) {
        super(context);
        this.isJobCompleted = false;
    }

    /**
     * Write the context to the file
     *
     */
    public void write(KEYOUT key, VALUEOUT value) {
        // write value to socket output stream
        String keyValuePair = key.toString() + "\t" + value.toString();
        writer.println(keyValuePair);
    }

    public void setupConnectionToNamenode() {
        // create the socket to Namenode Service
        try {
            socket = new Socket(this.getNamenodeServerIP(), NamenodeService.NAMENODE_PORT);

            if (socket != null) {
                writer = new PrintWriter(socket.getOutputStream(), true);
                writer.println(NamenodeService.REDUCERCLIENT_IDENTIFIER);
                writer.println(this.getOutputDir());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public Socket getSocket() {
        return socket;
    }
}
