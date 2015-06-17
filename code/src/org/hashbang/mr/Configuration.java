package org.hashbang.mr;

/**
 *
 */

import java.io.Serializable;
import java.util.HashMap;

/**
 * Provides access to configuration parameters.
 */
public class Configuration implements Serializable {

    private HashMap<String, String> conf;

    public Configuration() {
        conf = new HashMap<String, String>();
        conf.put("io.file.blocksize", "64");
    }

    /**
     * Get the value of the <code>name</code> property, <code>null</code> if
     * no such property exists.
     * <p/>
     * @param name the property name, will be trimmed before get value.
     * @return the value of the <code>name</code> or its replacing property,
     * or null if no such property exists.
     */

    public String get(String name) {
        return conf.get(name);
    }

    /**
     * Set the <code>value</code> of the <code>name</code> property.
     * @param name  property name.
     * @param value property value.
     */

    public void set(String name, String value) {

    }

    /**
     *  Unset a previously set property.
     * @param name
     */

    public void unset(String name) {

    }

    /**
     * @return Returns an associative array of default configurations
     */
    public HashMap<String,String> getDefaults() {
        return conf;
    }

}