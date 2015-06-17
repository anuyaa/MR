package org.hashbang.fs;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.Socket;

/**
 * Created by alwin on 4/7/15.
 */
public class NamenodeClient {
    private String serverIP;

    public NamenodeClient(String server) {
        this.serverIP = server;
    }

    /**
     * Connects to Namenode server and fetches size bytes data from offset
     *
     * @param fileName
     * @param offset
     * @param size
     * @return
     */
    public byte[] getData(String fileName, long offset, int size) {
        byte buffer[] = null;
        try {
            Socket socket = new Socket(serverIP, NamenodeService.NAMENODE_PORT);
            PrintWriter out =
                    new PrintWriter(socket.getOutputStream(), true);
            DataTransferUtil util = new DataTransferUtil();
            out.println(NamenodeService.MAPCLIENT_IDENTIFIER);
            out.println(fileName);
            out.println(offset);
            out.println(size);
            buffer = util.readStreamToBuffer(socket.getInputStream());
        } catch (IOException e) {
            e.printStackTrace();
        }
        return buffer;
    }
}
