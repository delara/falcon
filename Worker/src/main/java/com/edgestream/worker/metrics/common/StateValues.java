package com.edgestream.worker.metrics.common;

import java.io.Serializable;

public class StateValues implements Serializable {
    private String key;
    private String keyType;
    private double rate;
    private long counter;

    public StateValues(String key, String keyType, double rate, long counter) {
        this.key = key;
        this.keyType = keyType;
        this.rate = rate;
        this.counter = counter;
    }


    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public double getRate() {
        return rate;
    }

    public void setRate(double rate) {
        this.rate = rate;
    }

    public long getCounter() {
        return counter;
    }

    public void setCounter(long counter) {
        this.counter = counter;
    }

    public String getKeyType() {
        return keyType;
    }

    public void setKeyType(String keyType) {
        this.keyType = keyType;
    }
}
