package com.edgestream.worker.metrics.common;

import java.io.Serializable;
import java.util.HashMap;

public class StateCollection implements Serializable {
    private HashMap<String, StateValues> stateCollection = new HashMap<>();

    public StateCollection() {
    }

    public HashMap<String, StateValues> getStateCollection() {
        return stateCollection;
    }

    public void setStateCollection(HashMap<String, StateValues> stateCollection) {
        this.stateCollection = stateCollection;
    }

    public StateValues getKey(String key) {
        return this.getStateCollection().get(key);
    }

    public void put(String key, StateValues value) {
        this.getStateCollection().put(key, value);
    }

    public boolean containsKey(String key) {
        return this.getStateCollection().containsKey(key);
    }

    public boolean isEmpty(){
        return this.getStateCollection().isEmpty();
    }
}
