package com.edgestream.worker.operator;

import com.edgestream.worker.common.Tuple;
import org.apache.activemq.artemis.api.core.ActiveMQException;

public interface SingleTupleEmitter {

    void emit(Tuple tupleToEmit, String tupleID, String tupleInternalID, String producerId, String timeStamp, String tupleOrigin, String inputKey) throws ActiveMQException;


}
