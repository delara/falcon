package com.edgestream.application.vehiclestats.operators;

import com.edgestream.worker.common.Tuple;
import com.edgestream.worker.metrics.metricscollector2.MetricsCollector3;
import com.edgestream.worker.operator.*;

import java.util.ArrayList;
import java.util.HashMap;

public class StatsAggregatorWindowStateless extends StatelessWindowOperator {
    HashMap<String, Double> vehicleStats;
    String windowResult;

    public StatsAggregatorWindowStateless(OperatorID operatorID, String inputType, String metrics_broker_IP_address, MetricsCollector3 metricsCollector) {
        super(operatorID, inputType, metricsCollector);
        vehicleStats = new HashMap<>();
        windowType="tumbling";
        initialiseTumblingWindowObject(10000, operatorID, metrics_broker_IP_address);
    }

    @Override
    public void processWindow(ArrayList window) {
        Integer sum = 0;
        for (Object element: window) {
            String elementString = (String) element;
            sum += Integer.valueOf(elementString);
        }
        Double avg = Double.valueOf(sum / window.size());
        System.out.println("Average speed over the window:" + avg);
        windowResult = avg.toString();
    }

    @Override
    public void emitWindowTuple(Tuple tuple, String tupleID, String tupleInternalID, String producerId, String tupleOrigin, String inputKey) {
        tuple.setType("D");
        tuple.setPayloadAsByteArray(windowResult.getBytes());
        emit(tuple, tupleID, tupleInternalID, producerId, windowStartTimestamp, tupleOrigin, inputKey);
        windowStartTimestamp = null;
    }

    @Override
    public ArrayList<String> processWindowElement(Tuple tuple, String tupleID, String tupleInternalID, String producerId, String timeStamp, String tupleOrigin, String inputKey) {
        System.out.println("Processing tuple");
        String tupleData = new String(tuple.getPayloadAsByteArray());
        String vehicleId = tupleData.split(",")[0];
        String vehicleSpeed =  String.valueOf(Double.parseDouble(tupleData.split(",")[1]));
        if (windowStartTimestamp == null) {
            windowStartTimestamp = timeStamp;
        }
        ArrayList<String> window = updateWindowState(vehicleSpeed);
        String stringPayload = window.toString();
        System.out.println("Current window: " + stringPayload);
//        try {
//            Thread.sleep(5);
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
        return window;
    }
}
