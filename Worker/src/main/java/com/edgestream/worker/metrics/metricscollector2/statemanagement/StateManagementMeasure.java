package com.edgestream.worker.metrics.metricscollector2.statemanagement;

import com.edgestream.worker.metrics.metricscollector2.state.StateMeasure;
import com.edgestream.worker.metrics.metricscollector2.utils.GenericMeasure;

public class StateManagementMeasure  extends GenericMeasure {
    public enum OperationType {
        Checkpointing,
        OnDemandBackup,
        Restore
    }

    private OperationType operationType;
    private String keys;
    private long sizeBytes;
    private long items;
    private long duration;

    public StateManagementMeasure(OperationType operationType, String keys, long sizeBytes, long items, long duration) {
        super(System.currentTimeMillis());
        this.operationType = operationType;
        this.keys = keys;
        this.sizeBytes = sizeBytes;
        this.items = items;
        this.duration = duration;
    }

    public OperationType getOperationType() {
        return operationType;
    }

    public void setOperationType(OperationType operationType) {
        this.operationType = operationType;
    }

    public String getKeys() {
        return keys;
    }

    public void setKeys(String keys) {
        this.keys = keys;
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

    public long getDuration() {
        return duration;
    }

    public void setDuration(long duration) {
        this.duration = duration;
    }
}
