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
import java.util.HashMap;

public class StatelessWindowOperator extends Operator implements Serializable, SingleTupleProcessing, SingleTupleEmitter {
    public String windowType;
    int windowSize;
    public ArrayList<String> tumblingWindowStateObject;
    public String windowStartTimestamp = null;
    public StateCollector stateCollector;

    public StatelessWindowOperator(OperatorID operatorID, String inputType, MetricsCollector3 metricsCollector){
        super(operatorID, inputType, metricsCollector);
        this.stateCollector = this.metricsCollector.getStateCollector();
    }

    private void addElementToWindow(String windowElement) {
        tumblingWindowStateObject.add(windowElement);
    }

    public void emptyWindow() {
        tumblingWindowStateObject.clear();
    }

    public void initialiseTumblingWindowObject(int windowSize, OperatorID operatorID, String metrics_broker_IP_address) {
        tumblingWindowStateObject = new ArrayList<>();
        this.windowSize = windowSize;
    }

    private boolean checkIfWindowIsFull() {
        return tumblingWindowStateObject.size() == windowSize;
    }

    public ArrayList<String> updateWindowState(String windowElement) {
        addElementToWindow(windowElement);
        if (checkIfWindowIsFull()) {
            ArrayList<String> window = (ArrayList<String>) tumblingWindowStateObject.clone();
            emptyWindow();
            return window;
        }
        return null;
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
