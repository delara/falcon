package com.edgestream.worker.metrics.metricscollector2.utils;

public abstract class GenericMeasure {
    private long timestamp;
    private Long id;

    public GenericMeasure(long timestamp) {
        this.timestamp = timestamp;
    }

    public GenericMeasure() {
        this.timestamp = System.currentTimeMillis();
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }
}
