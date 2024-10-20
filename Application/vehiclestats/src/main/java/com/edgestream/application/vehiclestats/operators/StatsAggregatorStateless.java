package com.edgestream.application.vehiclestats.operators;

import com.edgestream.worker.common.Tuple;
import com.edgestream.worker.metrics.metricscollector2.MetricsCollector3;
import com.edgestream.worker.operator.OperatorID;
import com.edgestream.worker.operator.Operator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

public class StatsAggregatorStateless extends Operator {
    HashMap<String, ArrayList<Double>> vehicleStats;

    public StatsAggregatorStateless(OperatorID operatorID, String inputType, MetricsCollector3 metricsCollector) {
        super(operatorID, inputType, metricsCollector);
        vehicleStats = new HashMap<>();
    }

    public String convertHashMapToStringPayload() {
        String concatenatedResult = "";
        for (String key: vehicleStats.keySet()) {
            concatenatedResult = concatenatedResult + key + ":" + vehicleStats.get(key).toString() + ",";
        }
        return concatenatedResult;
    }

    public void stateUpdate(String vehicleId, Double vehicleSpeed) {
        if (vehicleStats.containsKey(vehicleId)) {
            List<Double> stats = vehicleStats.get(vehicleId);
            Double prevAvg = stats.get(0);
            Double vehicleCount = stats.get(1);
            Double newSum = (prevAvg * vehicleCount) + vehicleSpeed;
            Double newCount = vehicleCount + 1;
            Double newAvg = newSum / newCount;
            vehicleStats.put(vehicleId, new ArrayList<>(Arrays.asList(newAvg, newCount)));
        } else {
            vehicleStats.put(vehicleId, new ArrayList<>(Arrays.asList(vehicleSpeed, 1.0)));
        }
    }

    @Override
    public void processTuple(Tuple tuple, String tupleID, String tupleInternalID, String producerId, String timeStamp, String tupleOrigin, String inputKey) {
//        System.out.println("Processing tuple");
        long processingStart = System.currentTimeMillis();
        String tupleData = new String(tuple.getPayloadAsByteArray());
//        String tID = tupleData.split(",")[0];
        String vehicleId = tupleData.split(",")[0];
        System.out.println("Processing tuple having id: " + vehicleId);
        Double vehicleSpeed = Double.parseDouble(tupleData.split(",")[1]);
        stateUpdate(vehicleId, vehicleSpeed);
        String stringPayload = convertHashMapToStringPayload();
        tuple.setType("D");
        tuple.setPayloadAsByteArray(stringPayload.getBytes());
        long processingEnd = System.currentTimeMillis();
//        System.out.println("Processing latency: " + (processingEnd-processingStart) + " ms");
//        System.out.println("Vehicle stats string: " + stringPayload);
        emit(tuple, tupleID, tupleInternalID, producerId, timeStamp, tupleOrigin, inputKey);
    }
}
