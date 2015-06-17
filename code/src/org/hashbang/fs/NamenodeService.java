package org.hashbang.fs;


import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by ankita on 3/31/15.
 * NamenodeService, this class listens to request, Serve the request according to the Client.
 */
public class NamenodeService {

    public static final int NAMENODE_PORT = 9999; // dummy port number
    public static final String MAPCLIENT_IDENTIFIER = "MAP_CLIENT";
    public static final String REDUCERCLIENT_IDENTIFIER = "REDUCER_CLIENT";

    public static void main(String[] args) {

        ServerSocket listener = null;

        // Implement thread pool
        List<DataProvider> dataProviders = new ArrayList<DataProvider>();

        List<DataReceiver> dataRecievers = new ArrayList<DataReceiver>();

        try {
            listener = new ServerSocket(NAMENODE_PORT);

            try {
                while (true) {
                    Socket clientSocket = listener.accept();
                    BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                    String identifier = in.readLine();

                    if (identifier.contentEquals(REDUCERCLIENT_IDENTIFIER)) {

                        DataReceiver dataReceiver = new DataReceiver(clientSocket);
                        dataRecievers.add(dataReceiver);
                        dataReceiver.start();

                    } else if (identifier.contentEquals(MAPCLIENT_IDENTIFIER)) {

                        DataProvider dataProvider = new DataProvider(clientSocket, in);
                        dataProviders.add(dataProvider);
                        dataProvider.start();
                    }
                }
            } finally {
                listener.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

/**
 * Created by ankita on 3/31/15.
 * DataProvider, this dedicated thread serves the request of input split to the task tracker
 */
class DataProvider extends Thread {

    Socket socket;
    DataTransferUtil util;
    BufferedReader in;

    public DataProvider(Socket given_socket, BufferedReader reader) {
        socket = given_socket;
        in = reader;
        util = new DataTransferUtil();
    }

    @Override
    public void run() {
        try {
            String filepath = in.readLine();
            Path path = Paths.get(filepath);

            long fileOffset = Long.parseLong(in.readLine());
            int blocksize = Integer.parseInt(in.readLine());

            byte[] data = util.readFileToBuffer(path, fileOffset, blocksize);
            OutputStream out = socket.getOutputStream();
            util.writeFromBufferToStream(data, out);
            out.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}


/**
 * Created by ankita on 3/31/15.
 * DataReceiver, this dedicated thread fetches data from reducer and write it to
 * the output directory specified by job client
 *
 */
class DataReceiver extends Thread {

    Socket socket;
    DataTransferUtil util;

    public DataReceiver(Socket given_socket) {
        socket = given_socket;
        util = new DataTransferUtil();
    }

    @Override
    public void run() {

        BufferedReader in = null;

        //step 1. read the output directory path
        //step 2. create output directory
        //step 3. create file name part-0000
        // while(true){
        //   read the key value pair and write it to the file.
        // }
        try {

            in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            String outputDirPath = in.readLine();
            System.out.println("outpath path: " + outputDirPath);
            String outputFilePath = outputDirPath + "/output";

            Files.createDirectories(Paths.get(outputFilePath));
            outputFilePath += "/job.output";

            PrintWriter writer = new PrintWriter(new FileOutputStream(outputFilePath), true);
            String keyValuePair;

            while ((keyValuePair = in.readLine()) != null) {
                writer.println(keyValuePair);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (in != null) {
                    in.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}