package com.edgestream.worker.common;



import com.edgestream.worker.runtime.reconfiguration.state.AtomicKey;
import com.edgestream.worker.runtime.reconfiguration.state.RoutingKey;

import java.io.Serializable;
import java.util.ArrayList;

public class TupleHeader implements Serializable {


    private String timeStampFromSource;
    private String timestampTransfer;
    private String previousOperator;
    private String tupleOrigin;
    private String tupleID;
    private RoutingKey routingKey;
    private String producerId;

    public TupleHeader() {
    }

    public TupleHeader(String tupleID, String timeStampFromSource, String timestampTransfer, String previousOperator, String tupleOrigin, String producerId) {
        this.timeStampFromSource = timeStampFromSource;
        this.timestampTransfer = timestampTransfer;
        this.previousOperator = previousOperator;
        this.tupleOrigin = tupleOrigin;
        this.tupleID = tupleID;
        this.producerId = producerId;
    }

    public String getTupleOrigin() {
        return tupleOrigin;
    }


    public String getTimeStampFromSource() {
        return timeStampFromSource;
    }

    public String getTimestampTransfer() {
        return timestampTransfer;
    }

    public String getPreviousOperator() {
        return previousOperator;
    }

    public String getTupleID() {
        return tupleID;
    }

    public void setTupleID(String tupleID) {
        this.tupleID = tupleID;
    }

    public String getProducerId() { return producerId;}

    public void addRoutingKey(RoutingKey routingKey){

        this.routingKey = routingKey;

    }

    public ArrayList<AtomicKey> getAtomicKeys() {
        return routingKey.getAtomicKeyList();
    }
}
