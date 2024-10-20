package com.edgestream.worker.io;

import com.edgestream.worker.common.Tuple;
import com.edgestream.worker.metrics.metricscollector2.MetricsCollector2;
import com.edgestream.worker.metrics.metricscollector2.MetricsCollector3;


public interface MessageProducerClient {


    void onRequestToSend(Tuple tupleToSend, String tupleID, String tupleInternalID, String producerId, String timeStampFromSource, String operatorID, String tupleOrigin, String inputKey, boolean canEmit);

    @Deprecated
    void onRequestToSend(Tuple tupleToSend, String tupleID, String tupleInternalID, String timeStampFromSource, String operatorID, String tupleOrigin, String inputKey);

    void setOperatorIsInWarmUpPhase(boolean inWarmUpPhase);

    MetricsCollector3 getMetricsCollector();


    void tearDown();
}
