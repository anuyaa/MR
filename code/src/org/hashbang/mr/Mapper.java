package org.hashbang.mr;

import java.io.*;

/**
 * Created by ankita on 4/8/15.
 */
public class Mapper<KEYOUT, VALUEOUT> implements IMapper<KEYOUT, VALUEOUT> {

    /**
     * Called once at the beginning of the task.
     *
     * @param context
     */
    public void setup(MapContext context) {

    }

    /**
     * Called once for each key/value pair in the input split. Most applications
     * should override this, but the default is the identity function.
     *
     * @param key
     * @param value
     * @param context
     */
    public void map(Long key, String value, MapContext context) {
//        System.out.println("Key :" + key.toString() + " Value : " + value.toString()); //debug
//        context.write(key, value.length());
    }

    /**
     * Called once at the end of the task.
     *
     * @param context
     */
    public void cleanup(MapContext context) {
        context.persistAll();
    }

    /**
     * Runs {@link IMapper#setup} once and {@link IMapper#map} over each key-value pair
     * and finally runs {@link IMapper#cleanup}
     *
     * @param context
     */
    public void run(MapContext context) {

        // calling setup
        setup(context);

        long offset = 0;
        InputStream is = null;
        BufferedReader bfReader;
        try {
            is = new ByteArrayInputStream(context.getDataBuffer().array());
            bfReader = new BufferedReader(new InputStreamReader(is));
            String line;
            while ((line = bfReader.readLine()) != null) {
                try {
                    map(offset, line, context);
                } catch (Exception e) {
                    System.out.println(e.toString());
                }
                offset = offset + line.length() + 1;  // +1 for newline char
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (is != null) {
                    is.close();
                }

                // calling cleanup
                cleanup(context);

            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
