package com.edgestream.worker.metrics.metricscollector2.operator;

import com.edgestream.worker.metrics.metricscollector2.utils.GenericMeasure;

import java.time.ZonedDateTime;

public class OperatorMeasure extends GenericMeasure {
    private Long size;
    private ZonedDateTime creationTime;
    private Long processingTime;

    public OperatorMeasure(long timestamp, Long id, Long size, ZonedDateTime creationTime, Long processingTime) {
        this.setTimestamp(timestamp);
        this.setSize(size);
        this.setCreationTime(creationTime);
        this.setProcessingTime(processingTime);
        this.setId(id);
    }

    public OperatorMeasure(long timestamp, Long id, Long size) {
        this.setTimestamp(timestamp);
        this.setSize(size);
        this.setId(id);
    }

    public Long getSize() {
        return size;
    }

    public void setSize(Long size) {
        this.size = size;
    }

    public ZonedDateTime getCreationTime() {
        return creationTime;
    }

    public void setCreationTime(ZonedDateTime creationTime) {
        this.creationTime = creationTime;
    }

    public Long getProcessingTime() {
        return processingTime;
    }

    public void setProcessingTime(Long processingTime) {
        this.processingTime = processingTime;
    }

}
