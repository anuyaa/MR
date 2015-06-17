package org.hashbang.mr;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A view of the job that is provided to the tasks while they
 * are running.
 */
public class MapContext<KEYOUT, VALUEOUT> extends JobContext {

    private Map<KEYOUT, List<VALUEOUT>> mapContext;
    private static final int sizeThreshold = 100000;

    public MapContext(int jobID, Job job, long offset, int dataSize) {
        super(jobID, job, offset, dataSize);
        this.mapContext = new HashMap<>();
    }

    /**
     * Save the key-value pair in context
     *
     * @param key
     * @param value
     */
    public void write(KEYOUT key, VALUEOUT value) {
        List<VALUEOUT> valuesForKey = mapContext.get(key);

        // if no list is available for the key, create a new list
        if (valuesForKey == null) {
            valuesForKey = new ArrayList<VALUEOUT>();
        }
        valuesForKey.add(value);
        // persist the values to disk if list size goes above the threshold
        if (valuesForKey.size() >= sizeThreshold) {
            persistKeyValues(key, valuesForKey);
            valuesForKey = null; // Clearing the valuelist after persisting the values
        }
        mapContext.put(key, valuesForKey);
    }

    /**
     * Persist the values for a key to disk
     *
     * @param key
     * @param values
     */
    public void persistKeyValues(KEYOUT key, List<VALUEOUT> values) {

        // append the values to existing key files
        try {

            String fileName = this.getWorkingDirectory() + "/"
                    + JobContext.MAP_OUTPUT_FILE_PREFIX + key.toString()
                    + JobContext.MAP_OUTPUT_FILE_PREFIX
                    + InetAddress.getLocalHost().getHostAddress()
                    // ObjectOutputStream doesn't support appending to a file,
                    // hence using a random number here
                    + "_" + Math.random();
            ObjectOutputStream oos = new ObjectOutputStream(
                    new FileOutputStream(fileName, true));
            for (VALUEOUT value : values) {
                oos.writeObject(value);
            }
            oos.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Persist all data to disk (to be used in {@link Mapper#cleanup(MapContext)})
     */
    public void persistAll() {
        for(Map.Entry<KEYOUT, List<VALUEOUT>> entry : mapContext.entrySet()) {
            persistKeyValues(entry.getKey(), entry.getValue());
        }
    }
}
