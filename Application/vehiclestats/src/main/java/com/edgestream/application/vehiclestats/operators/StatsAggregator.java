package com.edgestream.application.vehiclestats.operators;

import com.edgestream.worker.common.Tuple;
import com.edgestream.worker.metrics.metricscollector2.MetricsCollector3;
import com.edgestream.worker.operator.OperatorID;
import com.edgestream.worker.operator.StatefulOperator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.edgestream.worker.runtime.reconfiguration.state.AtomicKey;
import com.edgestream.worker.storage.StateObject;

public class StatsAggregator extends StatefulOperator {
    StateObject.HashMapObject4 vehicleStats;

    public StatsAggregator(OperatorID operatorID, String inputType, ArrayList<AtomicKey> routingKeys, String metrics_broker_IP_address, MetricsCollector3 metricsCollector) {
        super(operatorID, inputType, routingKeys, metricsCollector);
        vehicleStats = new StateObject.HashMapObject4(operatorID.getOperatorID_as_String(), metrics_broker_IP_address);
    }

    public void stateUpdate(String vehicleId, Double vehicleSpeed) {
        if (vehicleStats.containsKey(vehicleId)) {
            List<Double> stats = vehicleStats.getKey(vehicleId);
            Double prevAvg = stats.get(0);
            Double vehicleCount = stats.get(1);
            Double newSum = (prevAvg * vehicleCount) + vehicleSpeed;
            Double newCount = vehicleCount + 1;
            Double newAvg = newSum / newCount;
            vehicleStats.putKey(vehicleId, new ArrayList<>(Arrays.asList(newAvg, newCount)));
        } else {
            vehicleStats.putKey(vehicleId, new ArrayList<>(Arrays.asList(vehicleSpeed, 1.0)));
        }
    }

//    public String convertHashMapToStringPayload() {
//        String concatenatedResult = "";
//        for (String key: vehicleStats.keySet()) {
//            concatenatedResult = concatenatedResult + key + ":" + vehicleStats.get(key).toString() + ",";
//        }
//        return concatenatedResult;
//    }

    @Override
    public void performRestore(ArrayList<AtomicKey> routingKeys) {
        vehicleStats.restorePartitionKeys(routingKeys);
        System.out.println("State after restore:" + vehicleStats.printContents());
        restoreSize = vehicleStats.stateSize;
    }

    @Override
    public void performOnDemandBackup(ArrayList<AtomicKey> routingKeys) {
        //Perform backup operation for only keys to be routed.
        System.out.println("State during on-demand backup:" + vehicleStats.printContents());
        vehicleStats.checkStateChanges(routingKeys);
        vehicleStats.pausePeriodicCheckpoint();
        backupSize = vehicleStats.stateSize;
    }

    @Override
    public boolean performCheckpoint() {
        // return value checks if checkpoint actually did any data transfer
        boolean dataTransferred = vehicleStats.checkStateChanges(null);
        backupSize = vehicleStats.stateSize;
        changedKeysSize = vehicleStats.changedKeysSize;
        return dataTransferred;
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
        String stringPayload = vehicleStats.printContents();
        tuple.setType("D");
        tuple.setPayloadAsByteArray(stringPayload.getBytes());
        long processingEnd = System.currentTimeMillis();
//        System.out.println("Processing latency: " + (processingEnd-processingStart) + " ms");
//        System.out.println("Vehicle stats string: " + stringPayload);
        if (!isOperatorMigrating) {
            emit(tuple, tupleID, tupleInternalID, producerId, timeStamp, tupleOrigin, inputKey);
        }
    }
}
