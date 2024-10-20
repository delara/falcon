package com.edgestream.application.vehiclestats.operators;

import com.edgestream.worker.common.Tuple;
import com.edgestream.worker.metrics.metricscollector2.MetricsCollector3;
import com.edgestream.worker.operator.*;

import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class StatsAggregatorWindow extends WindowOperator {
    HashMap<String, Double> vehicleStats;
    String windowResult;

    public StatsAggregatorWindow(OperatorID operatorID, String inputType, ArrayList routingKeys, String metrics_broker_IP_address, MetricsCollector3 metricsCollector) {
        super(operatorID, inputType, routingKeys, metricsCollector);
        vehicleStats = new HashMap<>();
        windowType="tumbling";
        initialiseTumblingWindowObject(10000, operatorID, metrics_broker_IP_address);
    }

    @Override
    public void processWindow(ArrayList window) {
        Integer sum = 0;
        for (Object element: window) {
            sum += (Integer) element;
        }
        Double avg = Double.valueOf(sum / window.size());
        System.out.println("Average speed over the window:" + avg);
        windowResult = avg.toString();
    }

    @Override
    public void emitWindowTuple(Tuple tuple, String tupleID, String tupleInternalID, String producerId, String tupleOrigin, String inputKey) {
        tuple.setType("D");
        tuple.setPayloadAsByteArray(windowResult.getBytes());
        if (!isOperatorMigrating) {
            emit(tuple, tupleID, tupleInternalID, producerId, windowStartTimestamp, tupleOrigin, inputKey);
        }
        windowStartTimestamp = null;
    }

    @Override
    public ArrayList<Integer> processWindowElement(Tuple tuple, String tupleID, String tupleInternalID, String producerId, String timeStamp, String tupleOrigin, String inputKey) {
        System.out.println("Processing tuple");
        String tupleData = new String(tuple.getPayloadAsByteArray());
        String vehicleId = tupleData.split(",")[0];
        int vehicleSpeed = (int) Double.parseDouble(tupleData.split(",")[1]);
        if (windowStartTimestamp == null) {
            windowStartTimestamp = timeStamp;
        }
        ArrayList<Integer> window = tumblingWindowStateObject.updateWindowState(vehicleId, vehicleSpeed);
        String stringPayload = tumblingWindowStateObject.printContents();
        System.out.println("Current window: " + stringPayload);
//        try {
//            Thread.sleep(5);
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
        return window;
    }
}
