package com.edgestream.application.vehiclestats.operators;

import com.edgestream.worker.common.Tuple;
import com.edgestream.worker.metrics.metricscollector2.MetricsCollector3;
import com.edgestream.worker.operator.Operator;
import com.edgestream.worker.operator.OperatorID;
import com.edgestream.worker.operator.SingleTupleProcessing;
import org.apache.commons.lang3.SerializationUtils;

import java.io.ByteArrayOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.time.ZonedDateTime;

public class CSVWriter extends Operator {

    @Override
    public void processTuple(Tuple tuple, String tupleID, String tupleInternalID, String producerId, String timeStamp, String tupleOrigin, String inputKey) {
        try {
            FileWriter fw = new FileWriter("/tmp/vehicle_stats", true);
            long currentTime = System.currentTimeMillis();
            long latency = currentTime - ZonedDateTime.parse(timeStamp).toInstant().toEpochMilli();
            fw.write(currentTime+","+latency+"\n");
            fw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        String tupleData = new String(tuple.getPayloadAsByteArray());
        System.out.println("Tuple Data:" + tupleData);
        /*********SEND THE DATA*********************/
        byte[] dataToSend = SerializationUtils.serialize(tupleData);
        Tuple tupleToSend = new Tuple();
        tupleToSend.setPayloadAsByteArray(dataToSend);
        tupleToSend.setType("NONE"); // outputs an S type Tuple


        byte[] tupleToSendASByte = SerializationUtils.serialize(tupleToSend);

        // Need to emit this for metrics collector - only for sink operator
        emitForSink(tupleToSend, timeStamp);

        //this.getMessageProducerClient().getMetricsCollector().AddOutputEventSize(tupleInternalID, Long.valueOf(tupleToSendASByte.length), inputKey, timeStamp, tupleToSend.isLiveTuple());
    }

    public CSVWriter(OperatorID operatorID, String inputType, MetricsCollector3 metricsCollector) {
        super(operatorID, inputType, metricsCollector);
    }
}
