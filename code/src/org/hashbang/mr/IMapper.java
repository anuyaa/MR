package org.hashbang.mr;

/**
 * Created by ankita on 4/8/15.
 *
 */

public interface IMapper<KEYOUT, VALUEOUT> {

    /**
     * Called once at the beginning of the task.
     */
    public void setup(MapContext context);

    /**
     * Called once for each key/value pair in the input split. Most applications
     * should override this, but the default is the identity function.
     */
    public void map(Long key, String value, MapContext context);

    /**
     * Called once at the end of the task.
     */
    public void cleanup(MapContext context);

    /**
     * Runs {@link IMapper#setup} once and {@link IMapper#map} over each key-value pair
     * and finally runs {@link IMapper#cleanup}
     */
    public void run(MapContext context) ;
}

