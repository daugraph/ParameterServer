package org.ps.core;

import java.util.HashMap;

public class Environment {

    private static final Environment instance = new Environment();
    private HashMap<String, String> kvs;

    public static Environment getInstance() {
        return instance;
    }

    public String find(String key) {
        if (kvs.containsKey(key)) {
            return kvs.get(key);
        }
        return System.getenv(key);
    }
}
