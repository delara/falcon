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

class ArtemisStatsProducer extends Thread {
    private final ClientProducer producer;
    private final ClientSession session;
    private final String ProducerID;
    private final ServerLocator locator;
    private final String topologyID;
    private final String tupleOrigin;
    private final DslJson<Object> json = new DslJson<Object>(Settings.withRuntime().allowArrayFormat(true).includeServiceLoader());
    private final String outputType;
    private final long productionRate;
    private final Kryo kryo = new Kryo();
    private final BufferedReader br ;
    private String timeStamp;


    private int batchCommitSize;
    private final int total_to_send =0;


    public ArtemisStatsProducer( String outputType, double production_rate, String target_IP_recipient, String dataSetPath, String fqqn, String topologyID, String tupleOrigin, String producerID, String timeStamp) throws Exception {
        System.out.println("Starting producer: ");
        this.outputType = outputType;
        this.topologyID = topologyID;
        this.ProducerID = producerID;
        // Seeding the producer with a fixed timestamp for event window expt
        this.timeStamp = timeStamp;

        this.locator = ActiveMQClient.createServerLocator(target_IP_recipient); //ex tcp://10.70.20.48:61616
/*        this.locator.setBlockOnNonDurableSend(false);
        this.locator.setBlockOnDurableSend(false);
        this.locator.setBlockOnAcknowledge(false);*/

        kryo.register(TupleHeader.class);
        kryo.register(byte[].class);
        kryo.register(Tuple.class);

        ClientSessionFactory factory = locator.createSessionFactory();
        session = factory.createTransactedSession();
        System.out.println("Connected to Artemis Broker");

        String FQQN = fqqn;
        producer = session.createProducer(FQQN);
        this.tupleOrigin = tupleOrigin;
        File file = new File(dataSetPath);
        br = new BufferedReader(new FileReader(file));
        this.productionRate = (long) (1000/production_rate);
    }

    @Override
    public void run() {

        boolean isActive = true;

        int sequenceCounter = 0;

        /** Statistics* */
        long currentRecordcount = 0l;
        long windowStartTime = System.nanoTime();
        long runningTotal = 0l;
        long experimentStartTime = System.nanoTime();

        /** Batch size configuration*/
        int batchSize = 1;
        //long desiredRuntime = 60000000000l * this.desired_runtime; // will run for ten minutes

        IDGenerator generator = new IDGenerator();
        try {
            System.out.println("Sending test tuples....");
            while (isActive) {
                ClientMessage test_operator_msg = session.createMessage(true);

                Tuple tupleToSend = new Tuple();
                UUID tupleID = IDGenerator.generateType1UUID();
                TupleHeader tupleHeader = new TupleHeader(tupleID.toString(), ZonedDateTime.now().toString(), ZonedDateTime.now().toString(), "", this.tupleOrigin, this.ProducerID);
                // Seeding with a fixed timestamp for event windows. Uncomment for actual metrics.
//                TupleHeader tupleHeader = new TupleHeader(tupleID.toString(), timeStamp, timeStamp, "", this.tupleOrigin, this.ProducerID);
                tupleToSend.setTupleHeader(tupleHeader);
//                String timestamp = new Timestamp(System.currentTimeMillis()).toString();
                String line = br.readLine();
                if (line == null) {
                    break;
                }
//                byte[] payload = SerializationUtils.serialize(detected);
                tupleToSend.setTupleHeader(tupleHeader);
                tupleToSend.setPayloadAsByteArray(line.getBytes());
                tupleToSend.setType(outputType);

                tupleToSend.setKeyField("vehicle_id");
                tupleToSend.setKeyValue(line.split(",")[0]);

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
                test_operator_msg.putStringProperty("vehicle_id", line.split(",")[0]);


                byte priority =0; //lowest priority
                test_operator_msg.setPriority(priority);


                producer.send(test_operator_msg);
                //bis.close();

                session.commit();
                Thread.sleep(productionRate);
                ZonedDateTime parsedTimeStamp = ZonedDateTime.parse(this.timeStamp);
                this.timeStamp = parsedTimeStamp.plus(productionRate, ChronoUnit.MILLIS).toString();


                /*****************Log the throughput*******************************/
                long windowEndTime = System.nanoTime();
                long windowDuration = windowEndTime - windowStartTime;

                if (windowDuration > 1000000000l) {

//                    writer.println(" ProducerID, SeqNumber, WindowDuration, Throughput, RunningTotal: " + this.ProducerID + " " + sequenceCounter + " " + windowDuration +" " +  currentRecordcount +" " +  runningTotal);
                    System.out.println("ProducerID :" + this.ProducerID + " sent this many tuples in one second: " + currentRecordcount + " The running total is: " + runningTotal + " Duration: " + windowDuration);
                    //reset the counter
                    currentRecordcount = 0;
                    windowStartTime = windowEndTime;
                    sequenceCounter++;
                } else {

                    currentRecordcount = currentRecordcount + batchSize;
                }

                runningTotal = runningTotal + batchSize;
                /*********************************************************************/

               /* if (System.nanoTime() - experimentStartTime > desiredRuntime) {
                    isActive = false;
                    System.out.println("Runtime has expired, shutting down experiment");
                }*/

                if (runningTotal == total_to_send) {
                    isActive = false;
                    System.out.println(runningTotal + " tuples sent, shutting down");
                }



            }

            session.close();

        } catch (ActiveMQException | InterruptedException | IOException e) {
            e.printStackTrace();
        }

    }
}