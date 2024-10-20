package com.edgestream.worker.operator;

import java.io.Serializable;

public class OperatorID implements Serializable {

    String operatorID;

    public OperatorID(String operatorID) {
        this.operatorID = operatorID;
    }

    public String getOperatorID_as_String() {
        return operatorID;
    }

}
