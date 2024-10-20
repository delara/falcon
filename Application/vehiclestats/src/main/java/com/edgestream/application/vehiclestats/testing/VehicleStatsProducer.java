package com.edgestream.application.vehiclestats.testing;

import com.dslplatform.json.DslJson;
import com.dslplatform.json.runtime.Settings;
import com.edgestream.worker.common.IDGenerator;
import com.edgestream.worker.common.Tuple;
import com.edgestream.worker.common.TupleHeader;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.client.*;

import java.awt.image.BufferedImage;
import java.io.*;
import java.sql.Timestamp;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.UUID;

class VehicleStatsProducer extends Thread {
    private final ClientProducer producer;
    private final ClientSession session;
    private final String ProducerID;
    private final ServerLocator locator;
    private final String topologyID;
    private final String tupleOrigin;
    private final String outputType;
    private final long productionRate;
    private final Kryo kryo = new Kryo();
    private int keyIdCounter = 0;
    private ArrayList<String> vehicleIds;


    public VehicleStatsProducer( String outputType, double production_rate, String target_IP_recipient, String fqqn, String topologyID, String tupleOrigin, String producerID) throws Exception {
        System.out.println("Starting producer: ");
        this.outputType = outputType;
        this.topologyID = topologyID;
        this.ProducerID = producerID;

        this.locator = ActiveMQClient.createServerLocator(target_IP_recipient); //ex tcp://10.70.20.48:61616
        this.locator.setBlockOnNonDurableSend(false);
        this.locator.setBlockOnDurableSend(false);
        this.locator.setBlockOnAcknowledge(false);

        kryo.register(TupleHeader.class);
        kryo.register(byte[].class);
        kryo.register(Tuple.class);

        ClientSessionFactory factory = locator.createSessionFactory();
        session = factory.createTransactedSession();
        System.out.println("Connected to Artemis Broker");

        String FQQN = fqqn;
        producer = session.createProducer(FQQN);
        this.tupleOrigin = tupleOrigin;
//        this.productionRate = (long) (1000/production_rate);
        this.productionRate = (long) production_rate;
//        System.out.println("Sleep time: " + this.productionRate + "ms");

//        String[] vehicleIds = {"A", "B", "C", "D", "E", "F", "G", "H", "I", "J"};
//        String[] vehicleIds = {"A"};
        this.vehicleIds = new ArrayList<>();
        for (int i=0; i<10; i++) {
            String key = "A" + i;
            this.vehicleIds.add(key);
        }
    }

    public static Double getRandomSpeed(Double min, Double max) {
        return (Math.random() * (max - min)) + min;
    }

    public String getTupleUniformDistribution() {
//        String key = vehicleIds[keyIdCounter];
        String key = vehicleIds.get(keyIdCounter);
        String value = getRandomSpeed(20.0, 80.0).toString();
        keyIdCounter++;
        if (keyIdCounter == vehicleIds.size()) keyIdCounter = 0;
        return key + "," + value;
    }

    @Override
    public void run() {

        boolean isActive = true;

        /** Statistics* */
        long currentRecordcount = 0l;
        long runningTotal = 0l;
        long experimentStartTime = System.nanoTime();

        /** Batch size configuration*/
        int batchSize = 1;
        int sequenceCounter = 0;
        long totalWindowTime = 1000000000l; // 1 second
        long numberOfTuplesSent = 1;

                IDGenerator generator = new IDGenerator();
        try {
            System.out.println("Sending test tuples....");
            while (isActive) {
                long windowStartTime = System.nanoTime();
//                long producerStartTime = System.currentTimeMillis();
                ClientMessage test_operator_msg = session.createMessage(true);

                Tuple tupleToSend = new Tuple();
                UUID tupleID = IDGenerator.generateType1UUID();
                TupleHeader tupleHeader = new TupleHeader(tupleID.toString(), ZonedDateTime.now().toString(), ZonedDateTime.now().toString(), "", this.tupleOrigin, this.ProducerID);
                // Seeding with a fixed timestamp for event windows. Uncomment for actual metrics.
//                TupleHeader tupleHeader = new TupleHeader(tupleID.toString(), timeStamp, timeStamp, "", this.tupleOrigin, this.ProducerID);
                tupleToSend.setTupleHeader(tupleHeader);
//                String timestamp = new Timestamp(System.currentTimeMillis()).toString();
//                byte[] payload = SerializationUtils.serialize(detected);
                String tuplePayload = getTupleUniformDistribution();
                tupleToSend.setTupleHeader(tupleHeader);
                tupleToSend.setPayloadAsByteArray(tuplePayload.getBytes());
                tupleToSend.setType(outputType);

                tupleToSend.setKeyField("vehicle_id");
                tupleToSend.setKeyValue(tuplePayload.split(",")[0]);

                //byte[] tupleAsByteArray = SerializationUtils.serialize(tupleToSend);


                Output output = new Output(1024, -1);  //KRYO CHANGE
                kryo.writeObject(output, tupleToSend);  //KRYO CHANGE
                output.close();  //KRYO CHANGE


                //ByteArrayInputStream bis = new ByteArrayInputStream(tupleAsByteArray);
                //test_operator_msg.setBodyInputStream(bis);

                byte[] bytesToSend = output.getBuffer();
                test_operator_msg.putBytesProperty("payload", bytesToSend);

                long timeStamp = System.currentTimeMillis();
                test_operator_msg.setTimestamp(timeStamp);
                test_operator_msg.putStringProperty("tupleType", outputType);
                test_operator_msg.putStringProperty("topologyID", this.topologyID);
                test_operator_msg.putStringProperty("timeStamp", ZonedDateTime.now().toString());
                test_operator_msg.putStringProperty("timestampTransfer", ZonedDateTime.now().toString());
                test_operator_msg.putStringProperty("previousOperator", "");
                test_operator_msg.putStringProperty("tupleID", tupleID.toString());
                test_operator_msg.putStringProperty("vehicle_id", tuplePayload.split(",")[0]);


                byte priority =0; //lowest priority
                test_operator_msg.setPriority(priority);


                producer.send(test_operator_msg);
                //bis.close();

                session.commit();
                currentRecordcount = currentRecordcount + batchSize;

                double sleepTime = totalWindowTime/(productionRate-numberOfTuplesSent);
                long sleepTimeMillis = (long) sleepTime/1000000;
                int sleepTimeNanos = (int) sleepTime%1000000;
//                System.out.println("Sleeping for " + sleepTimeMillis + "ms and " + sleepTimeNanos + "ns");
                Thread.sleep(sleepTimeMillis, sleepTimeNanos);

                long windowEndTime = System.nanoTime();
//                System.out.println("Actual processing time: " + (windowEndTime-windowStartTime) + "ns");
                totalWindowTime -= (windowEndTime-windowStartTime);

                if (totalWindowTime < 0 || (numberOfTuplesSent == productionRate-1)) {
                    System.out.println("Producer sent this many tuples in one second: " + numberOfTuplesSent);
                    numberOfTuplesSent = 0;
                    totalWindowTime = 1000000000l;
                }
                numberOfTuplesSent++;

                /* old way of producing tuples */

                /*****************Log the throughput*******************************/
//                long windowEndTime = System.nanoTime();
//                long windowDuration = windowEndTime - windowStartTime;
//
//                if (windowDuration > 1000000000l) {
//
////                    writer.println(" ProducerID, SeqNumber, WindowDuration, Throughput, RunningTotal: " + this.ProducerID + " " + sequenceCounter + " " + windowDuration +" " +  currentRecordcount +" " +  runningTotal);
//                    System.out.println("ProducerID :" + this.ProducerID + " sent this many tuples in one second: " + currentRecordcount + " The running total is: " + runningTotal + " Duration: " + windowDuration);
//                    //reset the counter
//                    currentRecordcount = 0;
//                    windowStartTime = windowEndTime;
//                    sequenceCounter++;
//                } else {
//
//                    currentRecordcount = currentRecordcount + batchSize;
//                }

                /* New way of producing tuples */
//                if (currentRecordcount >= productionRate) {
//                    long windowEndTime = System.nanoTime();
//                    long windowDuration = windowEndTime - windowStartTime;
//                    while(windowDuration < 1000000000l) {
//                        windowEndTime = System.nanoTime();
//                        windowDuration = windowEndTime - windowStartTime;
//                    }
//                    System.out.println("ProducerID sent this many tuples in one second: " + currentRecordcount + " The running total is: " + runningTotal + " Duration: " + windowDuration);
//                    runningTotal = runningTotal + currentRecordcount;
//                    //reset the counter
//                    currentRecordcount = 0;
//                    windowStartTime = windowEndTime;
//                }
//                long producerEndTime = System.currentTimeMillis();
//                System.out.println("Tuple production time: " + (producerEndTime-producerStartTime) + " ms");
            }

            session.close();

        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}