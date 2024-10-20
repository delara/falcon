package com.edgestream.application.vehiclestats.operators;

import com.edgestream.worker.common.Tuple;
import com.edgestream.worker.metrics.metricscollector2.MetricsCollector3;
import com.edgestream.worker.operator.*;
import com.edgestream.worker.runtime.reconfiguration.state.AtomicKey;
import com.edgestream.worker.storage.EventTimeWindowManager;
import com.edgestream.worker.storage.WindowStateObject;

import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class StatsAggregatorEventWindow extends WindowOperator {
    long windowFrequency = 1; // in seconds
    EventTimeWindowManager eventTimeWindowManager;
    String windowResult;

    public StatsAggregatorEventWindow(OperatorID operatorID, String inputType, ArrayList routingKeys, String metrics_broker_IP_address, MetricsCollector3 metricsCollector) {
        super(operatorID, inputType, routingKeys, metricsCollector);
        //vehicleStats = new HashMap<>();
        System.out.println("Initialising operator");
        eventTimeWindowManager = new EventTimeWindowManager(windowFrequency, routingKeys, getOperatorID().getOperatorID_as_String(), metrics_broker_IP_address);
    }

    @Override
    public void processElement(Tuple tuple, String tupleID, String tupleInternalID, String producerId, String timeStamp, String tupleOrigin, String inputKey) {
        processTuple(tuple, tupleID, tupleInternalID, producerId, timeStamp, tupleOrigin, inputKey);
    }

    @Override
    public void processWindow(ArrayList window) {
        Double sum = 0.0;
        for (Object element: window) {
            String elementString = (String) element;
            String vehicleSpeed =  elementString.split(",")[1];
            sum += Double.parseDouble(vehicleSpeed);
        }
        Double avg = sum / window.size();
        System.out.println("Average speed over the window:" + avg);
        windowResult = avg.toString();
    }

    @Override
    public void emitWindowTuple(Tuple tuple, String tupleID, String tupleInternalID, String producerId, String tupleOrigin, String inputKey) {
        tuple.setType("D");
        tuple.setPayloadAsByteArray(windowResult.getBytes());
        long processingDuration = System.currentTimeMillis() - ZonedDateTime.parse(windowStartTimestamp).toInstant().toEpochMilli();
        System.out.println("Processing duration for key " + inputKey + " :" + processingDuration + " ms" );
        emit(tuple, tupleID, tupleInternalID, producerId, windowStartTimestamp, tupleOrigin, inputKey);
        windowStartTimestamp = null;
    }

    @Override
    public ArrayList processWindowElement(Tuple tuple, String tupleID, String tupleInternalID, String producerId, String timeStamp, String tupleOrigin, String inputKey) {
        System.out.println("Processing tuple");
        ArrayList<String> window = eventTimeWindowManager.processTuple(tuple, producerId, timeStamp, inputKey);
//        String stringPayload = eventTimeWindowManager.printContent();
//        System.out.println("State of the window object: " + stringPayload);
        if (windowStartTimestamp == null) {
            windowStartTimestamp = timeStamp;
            System.out.println("Setting window timestamp for key " + inputKey +  " :" + windowStartTimestamp);
        }
        return window;
    }
}
