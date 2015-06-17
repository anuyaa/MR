package org.hashbang.tasktracker;

/**
 *
 */
public interface ITaskTracker {

    /**
     * Spawns a new jvm and starts the task (map/reduce)
     * @return Status of the spawn
     */
    public int spawnTask();

    /**
     *
     * Reports start, finish or failure of task
     */
    public void reportTask();

}
