package com.edgestream.worker.operator;

import com.edgestream.worker.common.Tuple;
import com.edgestream.worker.io.MessageProducerClient;

/**
 * A place holder operator that does nothing. This place holder operator is used to keep track of external operators
 * */


public class OperatorExternal extends Operator {




    public OperatorExternal(MessageProducerClient messageProducerClient, OperatorID operatorID, String inputType) {
        super(messageProducerClient, operatorID,inputType);


    }


    public void emit(Tuple tupleToEmit)  {

    }

    @Override
    public void processElement(Tuple tuple, String tupleID, String tupleInternalID, String producerId, String timeStamp, String tupleOrigin, String inputKey){

    }





}

