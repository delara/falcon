package com.edgestream.application.vehiclestats.operators;

import com.edgestream.worker.common.Tuple;
import com.edgestream.worker.metrics.metricscollector2.MetricsCollector3;
import com.edgestream.worker.metrics.metricscollector2.statemanagement.StateManagementMeasure;
import com.edgestream.worker.operator.OperatorID;
import com.edgestream.worker.operator.StatefulOperator;
import com.edgestream.worker.runtime.reconfiguration.state.AtomicKey;
import com.edgestream.worker.storage.StateObject;

import java.util.ArrayList;
import java.util.Arrays;

public class StatsAggregatorLargeState extends StatefulOperator {
    StateObject.HashMapObject5 vehicleStats;
    private Thread performOnDemandBackup;
    protected Object lock = new Object();

    public StatsAggregatorLargeState(OperatorID operatorID, String inputType, ArrayList<AtomicKey> routingKeys, String metrics_broker_IP_address, MetricsCollector3 metricsCollector) {
        super(operatorID, inputType, routingKeys, metricsCollector);
        vehicleStats = new StateObject.HashMapObject5(operatorID.getOperatorID_as_String(), metrics_broker_IP_address);

        performOnDemandBackup = new Thread(() -> {
            try {
                while (true) {
                    synchronized (lock) {
                        lock.wait();
                        vehicleStats.backupToPathstore();
                        backupSize = vehicleStats.stateSize;
                        this.stateManagementCollector.add(StateManagementMeasure.OperationType.OnDemandBackup, "", backupSize, Long.valueOf(routingKeys.size()), backupDuration);
                    }
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });
        performOnDemandBackup.setName("OnDemandBackup");
        performOnDemandBackup.start();
    }

    public String inflateString(int factor, String originalData) {
        StringBuilder sb = new StringBuilder();
        for (int i=0;i<factor;i++) {
            sb.append(originalData);
        }
        return sb.toString();
    }

    public void stateUpdate(String vehicleId, String vehicleData) {
        System.out.println("Updating state");
        if (vehicleStats.containsKey(vehicleId)) {
            ArrayList<String> elements = vehicleStats.getKey(vehicleId);
            elements.add(vehicleData);
            vehicleStats.putKey(vehicleId, elements);
        } else {
            vehicleStats.putKey(vehicleId, new ArrayList<>(Arrays.asList(vehicleData)));
        }
    }

    @Override
    public void performRestore(ArrayList<AtomicKey> routingKeys) {
        vehicleStats.restorePartitionKeys(routingKeys);
//        System.out.println("State after restore:" + vehicleStats.printContents());
        restoreSize = vehicleStats.stateSize;
    }

    @Override
    public void performOnDemandBackup(ArrayList<AtomicKey> routingKeys) {
        //Perform backup operation for only keys to be routed.
//        System.out.println("State during on-demand backup:" + vehicleStats.printContents());
        synchronized (lock) {
            vehicleStats.checkStateChanges(routingKeys);
            vehicleStats.pausePeriodicCheckpoint();
            lock.notify();
        }
    }

    @Override
    public boolean performCheckpoint() {
        System.out.println("Performing checkpoint");
        // return value checks if checkpoint actually did any data transfer
        boolean dataTransferred = vehicleStats.checkStateChanges(null);
        backupSize = vehicleStats.stateSize;
        changedKeysSize = vehicleStats.changedKeysSize;
        return dataTransferred;
//        return false;
    }

    @Override
    public void processTuple(Tuple tuple, String tupleID, String tupleInternalID, String producerId, String timeStamp, String tupleOrigin, String inputKey) {
//        System.out.println("Processing tuple");
        String tupleData = new String(tuple.getPayloadAsByteArray());
//        String tID = tupleData.split(",")[0];
//        System.out.println("Processing tuple having id: " + tID);
        String vehicleId = tupleData.split(",")[0];
        String vehicleData = tupleData.split(",")[1];
        long inflationStart = System.currentTimeMillis();
        vehicleData = inflateString(1200, vehicleData);
        long inflationDuration = System.currentTimeMillis()-inflationStart;
//        System.out.println("Processing duration for inflation: " + inflationDuration + "ms");
        stateUpdate(vehicleId, vehicleData);
//        String stringPayload = vehicleStats.printContents();
        tuple.setType("D");
        tuple.setPayloadAsByteArray(vehicleId.getBytes());
//        System.out.println("Vehicle stats string: " + stringPayload);
        if (!isOperatorMigrating) {
            emit(tuple, tupleID, tupleInternalID, producerId, timeStamp, tupleOrigin, inputKey);
        }
    }
}
