package com.edgestream.worker.metrics.common;

import java.io.Serializable;

public class InputEventMetric implements Serializable {

    private final String tupleID;
    private final String msgTimeCreation;
    private final Long event_size;
    private final String previousOperatorId;
    private final String transferringTime;
    private final String key;
    private final boolean liveTuple;
    private final String tupleInternalID;
    private final String tupleOrigin;

    public InputEventMetric(
            String tupleID
            , String msgTimeCreation
            , Long event_size
            , String previousOperatorId
            , String transferringTime
            , String key
            , boolean liveTuple
            , String tupleInternalID
            , String tupleOrigin) {


        this.tupleID = tupleID;
        this.msgTimeCreation = msgTimeCreation;
        this.event_size = event_size;
        this.previousOperatorId = previousOperatorId;
        this.transferringTime = transferringTime;
        this.key = key;
        this.liveTuple = liveTuple;
        this.tupleInternalID = tupleInternalID;
        this.tupleOrigin = tupleOrigin;
    }

    public String getTupleID() {
        return tupleID;
    }

    public String getMsgTimeCreation() {
        return msgTimeCreation;
    }

    public Long getEvent_size() {
        return event_size;
    }

    public String getPreviousOperatorId() {
        return previousOperatorId;
    }

    public String getTransferringTime() {
        return transferringTime;
    }

    public String getKey() {
        return key;
    }

    public boolean isLiveTuple() {
        return liveTuple;
    }

    public String getTupleInternalID() {
        return tupleInternalID;
    }

    public String getTupleOrigin() {
        return tupleOrigin;
    }
}
