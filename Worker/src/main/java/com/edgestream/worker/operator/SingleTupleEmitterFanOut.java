package com.edgestream.worker.operator;

import com.edgestream.worker.common.Tuple;
import org.apache.activemq.artemis.api.core.ActiveMQException;

public interface SingleTupleEmitterFanOut {

    void emit(Tuple tupleToEmit, String tupleID, String tupleInternalID, String timeStamp, String tupleOrigin, String outputType, String inputKey) throws ActiveMQException;


}
