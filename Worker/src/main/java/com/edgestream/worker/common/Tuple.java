package com.edgestream.worker.common;



import java.io.Serializable;

public class Tuple extends StreamElement implements Serializable {

    String topologyID;
    String type;
    String payload;
    byte[] payloadAsByteArray;
    TupleHeader tupleHeader;


    String keyField;
    String keyValue;
    boolean reconfigMarker = false;
    boolean stableMarker = false;
    boolean liveTuple = true;


    public boolean isLiveTuple() {
        return liveTuple;
    }

    public void setToWarmUpTuple() {
        this.liveTuple = false;
    }



    public String getPayload() {
        return payload;
    }

    public void setPayload(String payload) {
        this.payload = payload;
    }

    public byte[] getPayloadAsByteArray(){
        return payloadAsByteArray;
    }

    public void setPayloadAsByteArray(byte[] byteArray){

        this.payloadAsByteArray = byteArray;
    }


    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public TupleHeader getTupleHeader() {
        return tupleHeader;
    }

    public void setTupleHeader(TupleHeader tupleHeader) {
        this.tupleHeader = tupleHeader;
    }


    public String getKeyField() {
        return keyField;
    }

    public void setKeyField(String keyField) {
        this.keyField = keyField;
    }

    public String getKeyValue() {
        return keyValue;
    }

    public void setKeyValue(String keyValue) {
        this.keyValue = keyValue;
    }

    public boolean isReconfigMarker() {
        return reconfigMarker;
    }

    public boolean isStableMarker() {
        return stableMarker;
    }

    public void setReconfigMarker(boolean reconfigMarker) {
        this.reconfigMarker = reconfigMarker;
    }

    public void setStableMarker(boolean stableMarker) { this.stableMarker = stableMarker;}

    public String getTopologyID() {
        return topologyID;
    }

    public void setTopologyID(String topologyID) {
        this.topologyID = topologyID;
    }


}
