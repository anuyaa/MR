package org.hashbang.mr;

import java.util.List;

/**
 * Created by rmathews
 * Reduces a set of intermediate values which share a key to a smaller set of
 * values.  
 */
public interface IReducer<KEYIN, VALUEIN> {


    /**
     * Called once at the start of the task.
     */
    public void setup(ReduceContext context)  ;

    /**
     * This method is called once for each key. Most applications will define
     * their reduce class by overriding this method. The default implementation
     * is an identity function.
     */
    public void reduce(KEYIN key, List<VALUEIN> values, ReduceContext context);

    /**
     * Called once at the end of the task.
     */
    public void cleanup(ReduceContext context);

    /**
     * Runs {@link IReducer#setup} once and {@link IReducer#reduce} over each key-valuelist
     * and finally runs {@link IReducer#cleanup}
     */
    public void run(ReduceContext context) ;
}
