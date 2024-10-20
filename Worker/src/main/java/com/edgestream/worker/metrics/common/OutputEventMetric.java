package com.edgestream.worker.metrics.common;

import java.io.Serializable;

public class OutputEventMetric implements Serializable {

    private final String eventTimestamp;
    private final String tupleInternalID;
    private final long size;
    private final String inputKey;
    private final String timeStampFromSource;
    private final boolean isLiveTuple;


    public OutputEventMetric(String eventTimestamp, String tupleInternalID, long size, String inputKey, String timeStampFromSource, boolean isLiveTuple) {
        this.eventTimestamp = eventTimestamp;
        this.tupleInternalID = tupleInternalID;
        this.size = size;
        this.inputKey = inputKey;
        this.timeStampFromSource = timeStampFromSource;
        this.isLiveTuple = isLiveTuple;
    }


    public String getEventTimestamp() {
        return eventTimestamp;
    }

    public String getTupleInternalID() {
        return tupleInternalID;
    }

    public long getSize() {
        return size;
    }

    public String getInputKey() {
        return inputKey;
    }

    public String getTimeStampFromSource() {
        return timeStampFromSource;
    }

    public boolean isLiveTuple() {
        return isLiveTuple;
    }
}
