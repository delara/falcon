package com.edgestream.worker.metrics.metricscollector2.reconfiguration;

import com.edgestream.worker.metrics.metricscollector2.utils.GenericMeasure;

public class ReconfigurationMeasure extends GenericMeasure {
    public enum ReconfigOperation {
        ReconfigMarker,
        StableMarker,
        StartupTime
    }

    ReconfigOperation operation;
    long creationTimestamp;

    public ReconfigurationMeasure(ReconfigOperation operation, long creationTimestamp) {
        super(System.currentTimeMillis());
        this.operation = operation;
        this.creationTimestamp = creationTimestamp;
    }

    public ReconfigOperation getOperation() {
        return operation;
    }

    public void setOperation(ReconfigOperation operation) {
        this.operation = operation;
    }

    public long getCreationTimestamp() {
        return creationTimestamp;
    }

    public void setCreationTimestamp(long creationTimestamp) {
        this.creationTimestamp = creationTimestamp;
    }

}
