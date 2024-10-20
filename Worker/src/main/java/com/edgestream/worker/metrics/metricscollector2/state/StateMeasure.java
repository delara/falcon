package com.edgestream.worker.metrics.metricscollector2.state;

import com.edgestream.worker.metrics.metricscollector2.utils.GenericMeasure;

public class StateMeasure extends GenericMeasure {
    public enum StateType {
        Keyed,
        Window
    }

    private StateType stateType;
    private String key;
    private long sizeBytes;
    private long items;
    private long windowingTime;

    public StateMeasure(StateType stateType, String key, long sizeBytes, long items, long windowingTime) {
        super(System.currentTimeMillis());
        this.stateType = stateType;
        this.key = key;
        this.sizeBytes = sizeBytes;
        this.items = items;
        this.windowingTime = windowingTime;
    }

    public StateType getStateType() {
        return stateType;
    }

    public void setStateType(StateType stateType) {
        this.stateType = stateType;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public long getSizeBytes() {
        return sizeBytes;
    }

    public void setSizeBytes(long sizeBytes) {
        this.sizeBytes = sizeBytes;
    }

    public long getItems() {
        return items;
    }

    public void setItems(long items) {
        this.items = items;
    }

    public long getWindowingTime() {
        return windowingTime;
    }

    public void setWindowingTime(long windowingTime) {
        this.windowingTime = windowingTime;
    }
}
