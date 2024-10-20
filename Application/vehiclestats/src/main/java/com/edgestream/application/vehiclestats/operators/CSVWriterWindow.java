package com.edgestream.application.vehiclestats.operators;

import com.edgestream.worker.common.Tuple;
import com.edgestream.worker.metrics.metricscollector2.MetricsCollector3;
import com.edgestream.worker.operator.Operator;
import com.edgestream.worker.operator.OperatorID;
import com.edgestream.worker.operator.SingleTupleProcessing;

import java.io.FileWriter;
import java.io.IOException;

public class CSVWriterWindow extends Operator implements SingleTupleProcessing {


    @Override
    public void processElement(Tuple tuple, String tupleID, String tupleInternalID, String producerId, String timeStamp, String tupleOrigin, String inputKey) {

        if(tuple.isStableMarker()){
            System.out.println("[Operator]" + "Stable marker received.....");
            this.disableWarmUpPhase();
            this.reset(); //reset the internal state of this operator
            this.enableEmitter(); //allow this operator to start emitting instead of dropping tuples

        }else {
            processTuple(tuple, tupleID, tupleInternalID, producerId, timeStamp, tupleOrigin, inputKey);
        }
    }

    @Override
    public void processTuple(Tuple tuple, String tupleID, String tupleInternalID, String producerId, String timeStamp, String tupleOrigin, String inputKey) {
        try {
            FileWriter fw1 = new FileWriter("/tmp/window_stats", true);
            FileWriter fw2 = new FileWriter("/tmp/vehicle_stats", true);
            String tupleData = new String(tuple.getPayloadAsByteArray());
            System.out.println("Tuple Data:" + tupleData);
            if (tupleData.contains("|")) {
                fw1.write(tupleData.split("\\|")[0] + "\n");
                fw2.write(tupleData.split("\\|")[1] + ",");
            } else {
                fw1.write(tupleData + "\n");
            }
            fw1.close();
            fw2.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public CSVWriterWindow(OperatorID operatorID, String inputType, MetricsCollector3 metricsCollector) {
        super(operatorID, inputType, metricsCollector);
    }
}
