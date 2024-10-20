package com.edgestream.application.vehiclestats.operators;

import com.edgestream.worker.common.Tuple;
import com.edgestream.worker.metrics.metricscollector2.MetricsCollector3;
import com.edgestream.worker.metrics.metricscollector2.statemanagement.StateManagementMeasure;
import com.edgestream.worker.operator.OperatorID;
import com.edgestream.worker.operator.StatefulOperator;
import com.edgestream.worker.runtime.reconfiguration.state.AtomicKey;
import com.edgestream.worker.storage.PathstoreClient;
import com.edgestream.worker.storage.StateObject;

import java.util.ArrayList;
import java.util.Arrays;

public class StatsAggregatorLargeStateDummy extends StatefulOperator {
    StateObject.HashMapObject5 vehicleStats;
    private Thread performOnDemandBackup;
    protected Object lock = new Object();

//    public StatsAggregatorLargeStateDummy(OperatorID operatorID, String inputType, ArrayList<AtomicKey> routingKeys, String metrics_broker_IP_address, MetricsCollector3 metricsCollector) {
//        super(operatorID, inputType, routingKeys, metricsCollector);
//        vehicleStats = new StateObject.HashMapObject5(operatorID.getOperatorID_as_String(), metrics_broker_IP_address);
//        if (!isOperatorMigrating) {
//            System.out.println("Inserting data to pathstore");
//            String vehicleData = inflateString(20000000, "vehicleData");
//            System.out.println("Size of data: " + vehicleData.getBytes().length + " bytes");
//            PathstoreClient.insertState(operatorID.getOperatorID_as_String(), "A", vehicleData);
//            System.out.println("Insertion to pathstore completed");
//        }
//        System.out.println("Reading from pathstore");
//        String vehicleData = PathstoreClient.readState("A");
//        if (vehicleData != null) {
//            System.out.println("Size of data: " + vehicleData.getBytes().length + " bytes");
//        }
//        PathstoreClient.initialiseConnection();
//        System.out.println("Inserting data to pathstore");
//        PathstoreClient.insertState(operatorID.getOperatorID_as_String(), "B", "dummyData");
//        System.out.println("Reading from pathstore");
//        String dummyData = PathstoreClient.readState("B");
//        System.out.println("Value of B: " + dummyData);
//        System.out.println("Inserting data to pathstore");
//        PathstoreClient.insertState(operatorID.getOperatorID_as_String(), "B", "dummyData2");
//    }

    public StatsAggregatorLargeStateDummy(OperatorID operatorID, String inputType, ArrayList<AtomicKey> routingKeys, String metrics_broker_IP_address, MetricsCollector3 metricsCollector) {
        super(operatorID, inputType, routingKeys, metricsCollector);
        vehicleStats = new StateObject.HashMapObject5(operatorID.getOperatorID_as_String(), metrics_broker_IP_address);
        if (!isOperatorMigrating) {
            String vehicleData = inflateString(10, "vehicleData");
            System.out.println("Size of data: " + vehicleData.getBytes().length + " bytes");
            ArrayList<String> vehicleDataList = new ArrayList<>();
            vehicleDataList.add(vehicleData);
//            vehicleStats.putKey("A", vehicleDataList);
//            vehicleStats.putKey("B", vehicleDataList);
//            vehicleStats.putKey("C", vehicleDataList);
//            vehicleStats.putKey("D", vehicleDataList);
//            vehicleStats.putKey("E", vehicleDataList);
//            vehicleStats.putKey("F", vehicleDataList);
            for(int i=0;i<10;i++) {
                String keyId = "A" + i;
                vehicleStats.putKey(keyId, vehicleDataList);
            }
        }

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
        System.out.println("Updating state for key:" + vehicleId);
//        if (vehicleStats.containsKey(vehicleId)) {
//            ArrayList<String> elements = vehicleStats.getKey(vehicleId);
//            elements.add(vehicleData);
//            vehicleStats.putKey(vehicleId, elements);
//        } else {
//            vehicleStats.putKey(vehicleId, new ArrayList<>(Arrays.asList(vehicleData)));
//        }
    }

    @Override
    public void performRestore(ArrayList<AtomicKey> routingKeys) {
        vehicleStats.restorePartitionKeys(routingKeys);
        ArrayList<String> vehicleDataList = vehicleStats.getKey("A0");
        String vehicleData = vehicleDataList.get(0);
        System.out.println("Size of data: " + vehicleData.getBytes().length + " bytes");
//        System.out.println("State after restore:" + vehicleStats.printContents());
        restoreSize = vehicleStats.stateSize;
    }

    public void performBackupToPathstore() {

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
//        System.out.println("Performing checkpoint");
//        // return value checks if checkpoint actually did any data transfer
//        boolean dataTransferred = vehicleStats.checkStateChanges(null);
//        backupSize = vehicleStats.stateSize;
//        changedKeysSize = vehicleStats.changedKeysSize;
//        return dataTransferred;
        return false;
    }

    @Override
    public void processTuple(Tuple tuple, String tupleID, String tupleInternalID, String producerId, String timeStamp, String tupleOrigin, String inputKey) {
//        System.out.println("Processing tuple");
        String tupleData = new String(tuple.getPayloadAsByteArray());
//        String tID = tupleData.split(",")[0];
//        System.out.println("Processing tuple having id: " + tID);
        String vehicleId = tupleData.split(",")[0];
        String vehicleData = tupleData.split(",")[1];
//        long inflationStart = System.currentTimeMillis();
//        vehicleData = inflateString(3000, vehicleData);
//        long inflationDuration = System.currentTimeMillis()-inflationStart;
////        System.out.println("Processing duration for inflation: " + inflationDuration + "ms");
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
