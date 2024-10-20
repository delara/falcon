package com.edgestream.worker.io.artemis;


import com.edgestream.worker.common.Tuple;
import com.edgestream.worker.common.TupleHeader;
import com.edgestream.worker.io.MessageProducerClient;
import com.edgestream.worker.io.TupleProducer;
import com.edgestream.worker.metrics.common.OutputEventMetric;
import com.edgestream.worker.metrics.metricscollector2.MetricsCollector2;
import com.edgestream.worker.metrics.metricscollector2.MetricsCollector3;
import com.edgestream.worker.metrics.metricscollector2.io.ContainerOutputEventWriter;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.client.*;
import org.apache.commons.lang3.SerializationUtils;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import java.io.ByteArrayInputStream;
import java.io.Serializable;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;


public class ActiveMQMessageProducerClient implements MessageProducerClient, Serializable {

    String ProducerClientID;
    String outputQueue;
    ArtemisProducer primaryProducer;
    String topologyID;


    MetricsCollector3 metricsCollector;
    int batchSize;
    int confirmationWindowSize; // usually set to 3000000


    // Empty constructor
    public ActiveMQMessageProducerClient() {

    }

    public MetricsCollector3 getMetricsCollector() {
        return metricsCollector;
    }

    @Override
    public void tearDown() {
        this.primaryProducer.unbindWarmerSocket();
    }


    public ActiveMQMessageProducerClient(String ProducerClientID, String output_FQQN, String brokerIP, MetricsCollector3 metricsCollector, int producerBatchSize, int producerBufferSize, String topologyID, String metricsOutputEventPortNumber) {
        this.metricsCollector = metricsCollector;
        this.ProducerClientID = ProducerClientID;
        this.outputQueue = output_FQQN;
        this.confirmationWindowSize = producerBufferSize;
        this.batchSize = producerBatchSize;
        this.topologyID = topologyID;
        this.primaryProducer = new ArtemisProducer(ProducerClientID, outputQueue, brokerIP, batchSize, metricsCollector, this.confirmationWindowSize, this.topologyID,metricsOutputEventPortNumber);



    }


    @Override
    public void onRequestToSend(Tuple tupleToSend, String tupleID, String tupleInternalID, String producerId, String timeStampFromSource, String operatorID, String tupleOrigin, String inputKey, boolean canEmit) {

        if(!tupleToSend.isStableMarker()) { //never emit a stable marker to the broker
            if(canEmit) {
                this.primaryProducer.writeToOutputQueue(tupleToSend, tupleID, tupleInternalID, producerId, timeStampFromSource, operatorID, tupleOrigin, inputKey);
            }else{
                //send a copy to the warmer socket anyway
                this.primaryProducer.sendWarmUpCopy(tupleToSend, tupleID, tupleInternalID, producerId, timeStampFromSource, operatorID, tupleOrigin, inputKey);
            }
        }else{
            System.out.println("[ActiveMQMessageProducerClient]: Stable marker has been found, will NOT be forwarded to broker....");
        }
    }

    @Override
    public void onRequestToSend(Tuple tupleToSend, String tupleID, String tupleInternalID, String timeStampFromSource, String operatorID, String tupleOrigin, String inputKey) {
        /*if(!tupleToSend.isStableMarker()) { //never emit a stable marker to the broker
             this.primaryProducer.writeToOutputQueue(tupleToSend, tupleID, tupleInternalID, timeStampFromSource, operatorID, tupleOrigin, inputKey);
        }else{
            System.out.println("[ActiveMQMessageProducerClient]: Stable marker has been found, will NOT be forwarded to broker....");
        }*/
    }


    @Override
    public void setOperatorIsInWarmUpPhase(boolean inWarmUpPhase) {
        this.primaryProducer.notifyThatOperatorIsInWarmUpPhase(inWarmUpPhase);
    }


}


class ArtemisProducer implements TupleProducer {
    ClientProducer producer;
    ClientSession session;
    String ProducerID;
    ServerLocator locator;
    MetricsCollector3 metricsCollector;
    String topologyID;

    //Control system
    private final AtomicInteger commitCounter = new AtomicInteger(0);
    private ZonedDateTime lastReceiving;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicBoolean closed = new AtomicBoolean(false);
    final int LIMIT_TIME = 10000;
    private int batchSize;
    private boolean operatorIsInWarmUpPhase = true;

    private ZMQ.Socket pub;
    private ZContext context;
    //private AMQProducerPubReceiver AMQproducerPubReceiver;
    private int sendCounter= 0;

    private final ContainerOutputEventWriter containerOutputEventWriter;
    private final Kryo kryo = new Kryo(); //KRYO CHANGE


    public ArtemisProducer(String ProducerID, String outputAddress, String broker_ip, int batchSize, MetricsCollector3 metricsCollector, int confirmationWindowSize, String topologyID, String metricsOutputEventPortNumber) {
        this.metricsCollector = metricsCollector;
        this.ProducerID = ProducerID;
        this.batchSize = batchSize;
        this.topologyID = topologyID;
        this.setBatchSize(batchSize);

        System.out.println("[ActiveMQMessageProducerClient]: Starting producer: " + ProducerID);
        printWarmUpState();

        containerOutputEventWriter = new ContainerOutputEventWriter(metricsOutputEventPortNumber);


        try {
            this.locator = ActiveMQClient.createServerLocator(broker_ip);
            this.locator.setBlockOnNonDurableSend(false);
            this.locator.setBlockOnDurableSend(false);
            this.locator.setBlockOnAcknowledge(false);
            ClientSessionFactory factory = locator.createSessionFactory();
            session = factory.createTransactedSession();
            System.out.println("[ActiveMQMessageProducerClient]: Connected to Artemis Broker");

            session.start();
            producer = session.createProducer(outputAddress);

        } catch (Exception e) {
            e.printStackTrace();
        }

        setupWarmerSocket();


        kryo.register(TupleHeader.class); //KRYO CHANGE
        kryo.register(byte[].class); //KRYO CHANGE
        kryo.register(Tuple.class); //KRYO CHANGE

    }

    public void unbindWarmerSocket(){

        System.out.println("[ActiveMQMessageProducerClient]: Unbinding PUB socket ");
        pub.unbind("tcp://*:9001");
        pub.close();
    }


    private void setupWarmerSocket(){
        //ZMQ output socket setup. This will be used for warming only
        try {
            context = new ZContext();
            //This socket will publish the output of this operator
            System.out.println("[ActiveMQMessageProducerClient]: Creating pub socket...");
            pub = context.createSocket(SocketType.PUB);
            pub.bind("tcp://*:9001");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void notifyThatOperatorIsInWarmUpPhase(boolean inWarmUpPhase) {
        operatorIsInWarmUpPhase = inWarmUpPhase;
        printWarmUpState();
    }


    @Override
    public void printWarmUpState() {
        System.out.println("[ActiveMQMessageProducerClient]: Operator warm up state is: ["+operatorIsInWarmUpPhase+"]");
    }


    /**
     * This method allows the container to send a copy to warmer socket while itself is in a warm up state.
     * The purpose of this is to allow successor containers to warm up in parallel
     * @param tupleToSend
     * @param tupleID
     * @param tupleInternalID
     * @param timeStamp
     * @param operatorID
     * @param tupleOrigin
     * @param inputKey
     */
    public void sendWarmUpCopy(Tuple tupleToSend, String tupleID, String tupleInternalID, String producerId, String timeStamp, String operatorID, String tupleOrigin, String inputKey){

        String timestampTransfer = ZonedDateTime.now().toString();
        TupleHeader tupleHeader = new TupleHeader(tupleID, timeStamp, timestampTransfer, operatorID, tupleOrigin, producerId);
        tupleToSend.setTupleHeader(tupleHeader);
        //byte[] dataToSend = SerializationUtils.serialize(tupleToSend);

        Output output = new Output(1024, -1);  //KRYO CHANGE
        kryo.writeObject(output, tupleToSend); //KRYO CHANGE
        output.close();//KRYO CHANGE
        byte[] dataToSend = output.getBuffer(); //KRYO CHANGE

        sendCopyToPubSocket(dataToSend);

    }

    private void sendCopyToPubSocket(byte[] incomingPayloadByteArray){
        int numberOfTuplesToSkip = 1;
        if(sendCounter == numberOfTuplesToSkip) {
            this.pub.send(incomingPayloadByteArray,ZMQ.NOBLOCK); //for the warmup socket
            sendCounter = 0;
            //System.out.println("[ActiveMQMessageProducerClient]: " + "Attempted to send a copy of a tuple to port 9001" ); //for debug only
        }else{
            sendCounter++;
        }
    }




    public void writeToOutputQueue(Tuple tupleToSend, String tupleID, String tupleInternalID, String producerId, String timeStampFromSource, String operatorID, String tupleOrigin, String inputKey) {

        try {

            //1. create the message object
            ClientMessage msg_to_send = session.createMessage(true);

            //2. set the timestamp
            msg_to_send.setTimestamp(System.currentTimeMillis());
            //3. set the tuple type (as an attribute)
            String timestampTransfer = ZonedDateTime.now().toString();

            msg_to_send.putStringProperty("tupleType", tupleToSend.getType());
            msg_to_send.putStringProperty("timeStamp", timeStampFromSource);
            msg_to_send.putStringProperty("timestampTransfer", timestampTransfer);
            msg_to_send.putStringProperty("previousOperator", operatorID);
            msg_to_send.putStringProperty("topologyID", this.topologyID);
            msg_to_send.putStringProperty("tupleID", tupleID);

            if (tupleToSend.getKeyField() != null) {
                msg_to_send.putStringProperty(tupleToSend.getKeyField(),tupleToSend.getKeyValue());
                //msg_to_send.putStringProperty("keyValue", tupleToSend.getKeyValue());
            }



            /**for debugging only, not used by operator*/
            if (tupleToSend.isReconfigMarker()) {
                msg_to_send.putBooleanProperty("reconfigMarker", tupleToSend.isReconfigMarker());
            }


            //4. set the header, to be used by ZeroMQ
            TupleHeader tupleHeader = new TupleHeader(tupleID, timeStampFromSource, timestampTransfer, operatorID, tupleOrigin, producerId);
            tupleToSend.setTupleHeader(tupleHeader);


            //byte[] dataToSend = SerializationUtils.serialize(tupleToSend); //KRYO CHANGE

            Output output = new Output(1024, -1);  //KRYO CHANGE
            kryo.writeObject(output, tupleToSend); //KRYO CHANGE
            output.close();//KRYO CHANGE
            byte[] dataToSend = output.getBuffer(); //KRYO CHANGE




            //sends a copy of this payload to the warmer socket of this container port 9001
            sendCopyToPubSocket(dataToSend);


            //attached the byte array to an input stream
            //ByteArrayInputStream bis = new ByteArrayInputStream(dataToSend);  //KRYO CHANGE

            //msg_to_send.setBodyInputStream(bis); //KRYO CHANGE
            msg_to_send.putBytesProperty("payload",dataToSend);


            //5. send the message to the queue(this will read the byte stream and put it into the ActiveMQ msg)
            producer.send(msg_to_send);
            session.commit();
            //bis.close();//KRYO CHANGE

            //avoid calling the metrics collector for these types of tuples and cases
            if(!tupleToSend.isReconfigMarker() && !tupleToSend.isStableMarker()) {
                if(operatorIsInWarmUpPhase){
                    tupleToSend.setToWarmUpTuple();
                }

                // TODO: replace with call to new ZMQMetricsService
                //containerOutputEventWriter.sendOutputEvent(new OutputEventMetric(ZonedDateTime.now().toString(),tupleInternalID, Long.valueOf(dataToSend.length), inputKey, timeStampFromSource, tupleToSend.isLiveTuple()));

                //this.metricsCollector.AddOutputEventSize(tupleInternalID, Long.valueOf(dataToSend.length), inputKey, timeStampFromSource, tupleToSend.isLiveTuple());
                this.commitCounter.getAndIncrement();
                setLastReceiving(ZonedDateTime.now());
            }

        } catch (Exception e) {
            e.printStackTrace();

        }


    }




    /**
     * This method has the right sequence of calls to close and commit the producer session
     */
    public void close() {
        //Signal to stop the thread publishing thread
        running.set(false);
        //Wait until thread is closed
        while (!closed.get()) {

        }

        try {
            if (commitCounter.get() > 0) {
                Duration duration = Duration.between(getLastReceiving(), ZonedDateTime.now());
                long time = duration.toMillis();

                if ((time >= LIMIT_TIME || commitCounter.get() >= this.getBatchSize()) && !session.isClosed()) {
                    commitCounter.set(0);

                    session.commit();


                }
            }
            session.close();
        } catch (ActiveMQException e) {
            e.printStackTrace();
        }
    }

    //Gets/sets
    public ZonedDateTime getLastReceiving() {
        return lastReceiving;
    }

    public void setLastReceiving(ZonedDateTime lastReceiving) {
        this.lastReceiving = lastReceiving;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }




}
