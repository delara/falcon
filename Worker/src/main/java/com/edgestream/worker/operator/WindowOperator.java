package com.edgestream.worker.operator;

import com.edgestream.worker.common.Tuple;
import com.edgestream.worker.metrics.metricscollector2.MetricsCollector3;
import com.edgestream.worker.metrics.metricscollector2.state.StateCollector;
import com.edgestream.worker.metrics.metricscollector2.state.StateMeasure.StateType;
import com.edgestream.worker.runtime.reconfiguration.state.AtomicKey;
import com.edgestream.worker.storage.WindowStateObject;

import java.io.Serializable;
import java.time.ZonedDateTime;
import java.util.ArrayList;

public class WindowOperator extends StatefulOperator implements Serializable, SingleTupleProcessing, SingleTupleEmitter {
    public String windowType;
    public WindowStateObject.TumblingWindowStateObject tumblingWindowStateObject;
    public String windowStartTimestamp = null;
    public StateCollector stateCollector;
    public boolean registerKey = false; // Enable if you want to register a key for the downstream operator; typically for joins
    public ArrayList<String> processingKeyspace = new ArrayList<>(); //Maintains a list of all keys that are processed by the operator instance

    public WindowOperator(OperatorID operatorID, String inputType, ArrayList<AtomicKey> routingKeys, MetricsCollector3 metricsCollector){
        super(operatorID, inputType, routingKeys, metricsCollector);
        this.stateCollector = this.metricsCollector.getStateCollector();
    }

    public void initialiseTumblingWindowObject(int windowSize, OperatorID operatorID, String metrics_broker_IP_address) {
        tumblingWindowStateObject = new WindowStateObject.TumblingWindowStateObject(windowSize, operatorID.getOperatorID_as_String(), metrics_broker_IP_address, routingKeys);
    }

    @Override
    public void performOnDemandBackup(ArrayList<AtomicKey> routingKeys) {
        tumblingWindowStateObject.onDemandBackup(routingKeys, getOperatorID().getOperatorID_as_String());
        backupSize = tumblingWindowStateObject.stateSize;
        if (windowStartTimestamp != null) {
            tumblingWindowStateObject.updateWindowTimestamp("windowTime", windowStartTimestamp);
        }
    }

    @Override
    public void performRestore(ArrayList<AtomicKey> routingKeys) {
        tumblingWindowStateObject.restoreWindowObject(routingKeys);
        restoreSize = tumblingWindowStateObject.stateSize;
        windowStartTimestamp = tumblingWindowStateObject.restoreWindowTimestamp("windowTime");
    }

    @Override
    public boolean performCheckpoint() {
        // return value checks if checkpoint actually did any data transfer
        boolean dataTransferred = tumblingWindowStateObject.performCheckpoint();
        backupSize = tumblingWindowStateObject.stateSize;
        changedKeysSize = tumblingWindowStateObject.changedKeySize;
        return dataTransferred;
    }

    public long findWindowSizeInBytes(ArrayList window) {
        long size = 0;
        for (Object o : window) {
            size += o.toString().getBytes().length;
        }
        return size;
    }

    @Override
    public void processTuple(Tuple tuple, String tupleID, String tupleInternalID, String producerId, String timeStamp, String tupleOrigin, String inputKey) {
        ArrayList window = processWindowElement(tuple, tupleID, tupleInternalID, producerId, timeStamp, tupleOrigin, inputKey);
        if (window != null) {
            this.tupleProcessStartTime = ZonedDateTime.now();
            long windowCloseTimestamp = System.currentTimeMillis();
            long windowDuration = windowCloseTimestamp - ZonedDateTime.parse(windowStartTimestamp).toInstant().toEpochMilli();
            if (!isOperatorMigrating) {
                this.stateCollector.add(StateType.Window, "", findWindowSizeInBytes(window), window.size(), windowDuration);
            }
            if (registerKey) {
                if (!processingKeyspace.contains(inputKey)) {
                    System.out.println("Emitting registration tuple for key: " + inputKey);
                    processingKeyspace.add(inputKey);
                    tuple.setType(getDownstreamTupleType());
                    String tuplePayload = "register=" + inputKey;
                    tuple.setPayloadAsByteArray(tuplePayload.getBytes());
                    emit(tuple, tupleID, tupleInternalID, producerId, timeStamp, tupleOrigin, inputKey);
                }
            }
            processWindow(window);
            emitWindowTuple(tuple, tupleID, tupleInternalID, producerId, tupleOrigin, inputKey);
        }
    }

    public ArrayList processWindowElement(Tuple tuple, String tupleID, String tupleInternalID, String producerId, String timeStamp, String tupleOrigin, String inputKey) {
        return null;
    }

    public void processWindow(ArrayList window) {
    }

    public String getDownstreamTupleType() {return null;}

    public void emitWindowTuple(Tuple tupleToEmit, String tupleID, String tupleInternalID, String producerId, String tupleOrigin, String inputKey) {}
}
