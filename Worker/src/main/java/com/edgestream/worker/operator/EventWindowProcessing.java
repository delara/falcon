package com.edgestream.worker.operator;

import com.edgestream.worker.common.Tuple;
import org.apache.activemq.artemis.api.core.ActiveMQException;

import java.io.IOException;

public interface EventWindowProcessing {


    void processTuple(Tuple tuple, String tupleID, String tupleInternalID, String producerId, String timeStamp, String tupleOrigin, String inputKey) throws IOException, ActiveMQException;
}
