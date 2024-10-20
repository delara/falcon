package com.edgestream.worker.common;

import com.edgestream.worker.operator.OperatorID;

public class OperatorIDFormatter {

    public static String formatOperatorID(String inputType,String replicaID){

        return "OP_" + inputType +"_00" + replicaID;

    }
}
