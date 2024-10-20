package com.edgestream.worker.metrics.metricscollector2.stream;

import com.edgestream.worker.metrics.metricscollector2.utils.GenericMeasure;

public class StreamMeasure extends GenericMeasure {
    private String previousOperatorId;
    private long transferringTime;
    private long tupleSize;

    public StreamMeasure(long id, long timestamp, String previousOperatorId, long transferringTime, long tupleSize) {
        super(timestamp);
        this.previousOperatorId = previousOperatorId;
        this.transferringTime = transferringTime;
        this.tupleSize = tupleSize;
        this.setId(id);
    }

    public String getPreviousOperatorId() {
        return previousOperatorId;
    }

    public void setPreviousOperatorId(String previousOperatorId) {
        this.previousOperatorId = previousOperatorId;
    }

    public long getTransferringTime() {
        return transferringTime;
    }

    public void setTransferringTime(long transferringTime) {
        this.transferringTime = transferringTime;
    }

    public long getTupleSize() {
        return tupleSize;
    }

    public void setTupleSize(long tupleSize) {
        this.tupleSize = tupleSize;
    }
}
