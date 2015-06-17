package org.hashbang.mr;

import java.io.*;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Reduces a set of intermediate values which share a key to a smaller set of
 * values.
 */

public class Reducer<KEYIN, VALUEIN> implements IReducer<KEYIN, VALUEIN> {


    /**
     * Called once at the start of the task.
     */
    public void setup(ReduceContext context) {
        context.setupConnectionToNamenode();
    }

    /**
     * This method is called once for each key. Most applications will define
     * their reduce class by overriding this method. The default implementation
     * is an identity function.
     */
    public void reduce(KEYIN key, List<VALUEIN> values, ReduceContext context) {

    }

    /**
     * Called once at the end of the task.
     */
    public void cleanup(ReduceContext context) {
        Socket socket = context.getSocket();
        if (socket != null) {
            try {
                socket.close();
            } catch (IOException exc) {
                exc.printStackTrace();
            }
        }
    }

    /**
     * Runs {@link IReducer#setup} once and {@link IReducer#reduce} over each key-valuelist
     * and finally runs {@link IReducer#cleanup}
     */
    public void run(ReduceContext context) {
        setup(context);
        HashMap<String, List<String>> keyFiles = new HashMap<>();
        File mapperDir = new File(context.getWorkingDirectory());
        String fileName;
        String key;
        for (File mapperOutputFile : mapperDir.listFiles()) {
            fileName = mapperOutputFile.getName();
            if (fileName.startsWith(JobContext.MAP_OUTPUT_FILE_PREFIX)) {
                String filePrefix = fileName.substring(0, fileName.lastIndexOf(JobContext.MAP_OUTPUT_FILE_PREFIX));
                key = filePrefix.substring(JobContext.MAP_OUTPUT_FILE_PREFIX.length());
                List<String> files = keyFiles.get(key);
                if (files == null) {
                    files = new ArrayList<>();
                }
                files.add(fileName);
                keyFiles.put(key, files);
            }
        }

        // iterate over each list of files for a key and run reduce()
        for (Map.Entry<String, List<String>> entry : keyFiles.entrySet()) {
            key = entry.getKey();
            List<String> files = entry.getValue();
            List<VALUEIN> values = new ArrayList<>();
            for (String mapperOutputFile : files) {
                ObjectInputStream ois;
                try {
                    ois = new ObjectInputStream(new FileInputStream(
                            context.getWorkingDirectory() + "/" + mapperOutputFile));
                    VALUEIN value;
                    while ((value = (VALUEIN) ois.readObject()) != null) {
                        values.add(value);
                    }
                } catch (EOFException e) {
                    // reached end of file
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (ClassNotFoundException e) {
                    e.printStackTrace();
                }
            }
            // Call reduce() after getting all values for the current key
            try {
                reduce((KEYIN) key, values, context);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        cleanup(context);
    }
}
