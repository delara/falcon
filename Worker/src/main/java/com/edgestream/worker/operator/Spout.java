package com.edgestream.worker.operator;



import java.io.Serializable;

public class Spout implements Serializable {

    private final OperatorID spoutID;
    String outPutType;


    public Spout(OperatorID spoutID, String outPutType){

        this.spoutID = spoutID;
        this.outPutType  = outPutType;

    }

    public OperatorID getSpoutID() {
        return spoutID;
    }

    public String getOutPutType() {
        return this.outPutType;
    }


    public void activate(){


    }

    public void deactivate(){


    }


}
