package com.edgestream.application.vehiclestats.operators;

import com.edgestream.worker.common.Tuple;
import com.edgestream.worker.metrics.metricscollector2.MetricsCollector3;
import com.edgestream.worker.operator.Operator;
import com.edgestream.worker.operator.OperatorID;
import com.edgestream.worker.operator.SingleTupleEmitter;
import com.edgestream.worker.operator.SingleTupleProcessing;
import com.edgestream.worker.storage.WindowStateObject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class StatsAggregatorSlidingWindow extends Operator implements SingleTupleProcessing, SingleTupleEmitter {
    HashMap<String, Double> vehicleStats;
    WindowStateObject.SlidingWindowStateObject windowStateObject;
    public boolean isOperatorMigrating = false;

    public StatsAggregatorSlidingWindow(OperatorID operatorID, String inputType, ArrayList routingKeys, String metrics_broker_IP_address, MetricsCollector3 metricsCollector) {
        super(operatorID, inputType, metricsCollector);
        //vehicleStats = new HashMap<>();
        System.out.println("Initialising operator");
        vehicleStats = new HashMap<>();
        windowStateObject = new WindowStateObject.SlidingWindowStateObject(4, 2, operatorID.getOperatorID_as_String(), metrics_broker_IP_address, routingKeys);
        isOperatorMigrating = windowStateObject.isOperatorMigrating;
    }

    @Override
    public void emit(Tuple tupleToEmit, String tupleID, String tupleInternalID, String producerId, String timeStamp, String tupleOrigin, String inputKey) {
        getMessageProducerClient().onRequestToSend(tupleToEmit, tupleID, tupleInternalID, producerId, timeStamp, getOperatorID().getOperatorID_as_String(), tupleOrigin, inputKey, canEmit());
    }

    @Override
    public void processElement(Tuple tuple, String tupleID, String tupleInternalID, String producerId, String timeStamp, String tupleOrigin, String inputKey) {
        if (tuple.isReconfigMarker()) {
            if (isOperatorMigrating) {
                // Reconfig marker arrives at the operator replica during migration due to a bug.
                // This is a hacky solution to fix this.
                System.out.println("Reconfiguration request received while operator migration. Rejecting this.");
            } else {
                System.out.println("Reconfiguration request received. Initiating on-demand backup.");
                //System.out.println("Tuple routing keys: " + ReflectionToStringBuilder.toString(tuple.getTupleHeader().getRoutingKeys()));
                //Perform backup operation for only keys to be routed.
                windowStateObject.onDemandBackup(tuple.getTupleHeader().getAtomicKeys(), getOperatorID().getOperatorID_as_String());
            }
        } else if(tuple.isStableMarker() && this.isInWarmUpPhase()) {

            System.out.println("[Operator]" + "Stable marker received.....");

            emit(tuple, tupleID, tupleInternalID, producerId, timeStamp, tupleOrigin, inputKey); //bypass the process tuple function and directly emit
            this.disableWarmUpPhase();
            isOperatorMigrating = false;
            this.reset(); //reset the internal state of this operator
            this.enableEmitter(); //allow this operator to start emitting instead of dropping tuples
        }else {
            processTuple(tuple, tupleID, tupleInternalID, producerId, timeStamp, tupleOrigin, inputKey);
        }
    }

    public Double processWindow(List<Integer> window) {
        Integer sum = 0;
        for (Integer element: window) {
            sum += element;
        }
        Double avg = Double.valueOf(sum / window.size());
        System.out.println("Average speed over the window:" + avg);
        return avg;
    }

    @Override
    public void processTuple(Tuple tuple, String tupleID, String tupleInternalID, String producerId, String timeStamp, String tupleOrigin, String inputKey) {
//        System.out.println("Processing tuple");
//        String tupleData = new String(tuple.getPayloadAsByteArray());
//        String vehicleId = tupleData.split(",")[0];
//        Integer vehicleSpeed = Integer.parseInt(tupleData.split(",")[1]);
//        List<Integer> window = windowStateObject(vehicleId, vehicleSpeed);
//        String stringPayload = windowStateObject.printContents();
//        System.out.println("Current window: " + stringPayload);
//        if (window != null) {
//            Double avg = processWindow(window);
//            stringPayload = stringPayload + "|" + avg;
//        }
//        tuple.setType("D");
//        tuple.setPayloadAsByteArray(stringPayload.getBytes());
//        emit(tuple, tupleID, tupleInternalID, timeStamp, tupleOrigin, inputKey);
    }
}
