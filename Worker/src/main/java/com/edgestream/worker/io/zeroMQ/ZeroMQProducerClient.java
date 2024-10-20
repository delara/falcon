package com.edgestream.worker.io.zeroMQ;

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
import org.apache.commons.lang3.SerializationUtils;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import java.io.Serializable;
import java.time.ZonedDateTime;


public class ZeroMQProducerClient implements MessageProducerClient, Serializable {

    private final String ProducerClientID;
    private final String outputSocketPort;
    private final ZeroMQProducer primaryProducer;
    private String successorIP = "NONE";
    private final MetricsCollector3 metricsCollector;
    private final int batchSize;



    /**
     * This constructor is used when this operator has been started after the consumer. In this case the successorIP
     * is provided so that this producer can make an outbound connection to it.
     */
    public ZeroMQProducerClient(String ProducerClientID, String outputSocketPort, MetricsCollector3 metricsCollector, int producerBatchSize, String successorIP, String metricsOutputEventPortNumber) {
        this.metricsCollector = metricsCollector;
        this.ProducerClientID = ProducerClientID;
        this.outputSocketPort = outputSocketPort; // this is a port that will be used by consumers to consume data

        this.batchSize = producerBatchSize;
        this.successorIP = successorIP;

        this.primaryProducer = new ZeroMQProducer(ProducerClientID, outputSocketPort, metricsCollector, successorIP,metricsOutputEventPortNumber);


    }

    /**
     * This constructor is used when this operator is expected to be deployed along with other operators in the same data center and will be fused using ZeroMQ
     * It creates a port that listens for consumers that will connect to this operator and pull data from it.
     */
    public ZeroMQProducerClient(String ProducerClientID, String outputSocketPort, MetricsCollector3 metricsCollector, int producerBatchSize, String metricsOutputEventPortNumber) {
        this.metricsCollector = metricsCollector;
        this.ProducerClientID = ProducerClientID;
        this.outputSocketPort = outputSocketPort; // this is a predecessor port that will be used by consumers to consume data

        this.batchSize = producerBatchSize;
        this.primaryProducer = new ZeroMQProducer(ProducerClientID, outputSocketPort, metricsCollector,metricsOutputEventPortNumber);

    }
    @Override
    public void setOperatorIsInWarmUpPhase(boolean inWarmUpPhase){

       this.primaryProducer.notifyThatOperatorIsInWarmUpPhase(inWarmUpPhase);
    }

    @Override
    public MetricsCollector3 getMetricsCollector() {
        return metricsCollector;
    }

    @Override
    public void tearDown() {

    }


    @Override
    public void onRequestToSend(Tuple tupleToSend, String tupleID, String tupleInternalID, String producerId, String timeStampFromSource, String operatorID, String tupleOrigin, String inputKey, boolean canEmit) {
        if(canEmit) {
            this.primaryProducer.writeToZeroMqConsumer(tupleToSend, tupleID, tupleInternalID, producerId, timeStampFromSource, operatorID, tupleOrigin, inputKey);
        }else{
            //send a copy to the warmer socket anyway
            this.primaryProducer.sendWarmUpCopy(tupleToSend, tupleID, tupleInternalID, producerId, timeStampFromSource, operatorID, tupleOrigin, inputKey);
        }
    }

    @Override
    public void onRequestToSend(Tuple tupleToSend, String tupleID, String tupleInternalID, String timeStampFromSource, String operatorID, String tupleOrigin, String inputKey) {
        //this.primaryProducer.writeToZeroMqConsumer(tupleToSend, tupleID,tupleInternalID, timeStampFromSource, operatorID, tupleOrigin, inputKey);
    }


}


class ZeroMQProducer implements TupleProducer {

    private final String ProducerID;
    private final String outputSocketPort;

    private final MetricsCollector3 metricsCollector;
    private ZContext context;
    private final ZMQ.Socket sender;
    private boolean operatorIsInWarmUpPhase = true;


    //warmer configs
    private ZMQ.Socket pub;
    //private ZMQProducerPubReceiver zmqProducerPubReceiver;
    private int sendCounter= 0;

    private final ContainerOutputEventWriter containerOutputEventWriter;
    private final Kryo kryo = new Kryo(); //KRYO CHANGE
    private final int HWM =50;



    /***
     * This constructor is called when this operator is created before its successors.
     * Any successors will initiate the connection to this operator
     * @param ProducerID
     * @param outputSocketPort
     * @param metricsCollector
     */
    public ZeroMQProducer(String ProducerID, String outputSocketPort, MetricsCollector3 metricsCollector,String metricsOutputEventPortNumber) {
        System.out.println("[ZeroMQProducerClient]: First constructor called" );
        this.metricsCollector = metricsCollector;
        this.ProducerID = ProducerID;
        this.outputSocketPort = outputSocketPort; // should be 8000 + n
        this.context = new ZContext();
        //  Socket to send messages on
        this.sender = context.createSocket(SocketType.PUSH);
        this.sender.setHWM(HWM);

        containerOutputEventWriter = new ContainerOutputEventWriter(metricsOutputEventPortNumber);

        //outputSocketAddress should be the ip address of the container its running in,
        //consumers will connect to this ip to receive messages from this PUSH socket
        this.sender.bind("tcp://*:" + this.outputSocketPort);

        System.out.print("[ZeroMQProducerClient]: Starting ZeroMQ Producer: " + ProducerID);
        System.out.println("with the following configuration:"  + " outputSocketPort(THIS SHOULD BE 8000)[" + outputSocketPort +"]" + " IP[" + "this container" + "]" );
        printWarmUpState();
        setupWarmerSocket();


        kryo.register(TupleHeader.class);//KRYO CHANGE
        kryo.register(byte[].class); //KRYO CHANGE
        kryo.register(Tuple.class); //KRYO CHANGE
    }


    /**
     * This constructor is called when a valid successorIP has been provided.
     * This means that this operator has been started after its successors and must reach out to the successors
     * input port and setup a ZMQ socket connection.
     *
     * @param ProducerID
     * @param outputSocketPort
     * @param metricsCollector
     * @param successorIP
     */
    public ZeroMQProducer(String ProducerID, String outputSocketPort, MetricsCollector3 metricsCollector, String successorIP, String metricsOutputEventPortNumber) {
        System.out.println("[ZeroMQProducerClient]: Second constructor called" );
        this.metricsCollector = metricsCollector;
        this.ProducerID = ProducerID;
        this.outputSocketPort = outputSocketPort; // should be 8000 + n
        this.context = new ZContext();
        //  Socket to send messages on
        this.sender = context.createSocket(SocketType.PUSH);
        this.sender.setHWM(HWM);

        containerOutputEventWriter = new ContainerOutputEventWriter(metricsOutputEventPortNumber);

        //outputSocketAddress should be the ip address this container,
        //consumers will connect to this ip to receive messages from this PUSH socket
        this.sender.bind("tcp://*:" + this.outputSocketPort);


        //Since this operator was started after the consumers, it must call the connect function to establish the connection
        // The consumers will be listening on this port.
        String successorPort = "7002";
        this.sender.connect("tcp://" + successorIP + ":" + successorPort);
        System.out.print("[ZeroMQProducerClient]: Starting ZeroMQ Producer: " + ProducerID);
        System.out.println("with the following configuration:"  + " successorPort(THIS SHOULD BE 7002)[" + successorPort +"]" + " successorIP[" + successorIP + "]" );

        printWarmUpState();
        setupWarmerSocket();


        kryo.register(TupleHeader.class); //KRYO CHANGE
        kryo.register(byte[].class); //KRYO CHANGE
        kryo.register(Tuple.class); //KRYO CHANGE
    }


    private void setupWarmerSocket(){
        //ZMQ output socket setup. This will be used for warming only
        try {
            context = new ZContext();
            //This socket will publish the output of this operator
            System.out.println("[ZeroMQProducerClient]: Creating pub socket to publish the output of this operator...");
            pub = context.createSocket(SocketType.PUB);
            pub.bind("tcp://*:9001");

        } catch (Exception e) {
            e.printStackTrace();
        }

    }


    public void notifyThatOperatorIsInWarmUpPhase(boolean inWarmUpPhase){

        operatorIsInWarmUpPhase = inWarmUpPhase;
        printWarmUpState();
    }

    @Override
    public void printWarmUpState() {
        System.out.println("[ZeroMQProducerClient]: Operator warm up state is: [" +  operatorIsInWarmUpPhase+"]");

    }


    /**
     * This method allows the container to send a copy to warmer socket while itself is in a warmup state.
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

        if(!tupleToSend.isStableMarker()) {
            String timestampTransfer = ZonedDateTime.now().toString();
            TupleHeader tupleHeader = new TupleHeader(tupleID, timeStamp, timestampTransfer, operatorID, tupleOrigin, producerId);
            tupleToSend.setTupleHeader(tupleHeader);

            //byte[] dataToSend = SerializationUtils.serialize(tupleToSend);  //REPLACED WITH KRYO

            Output output = new Output(1024, -1);  //KRYO CHANGE
            kryo.writeObject(output, tupleToSend); //KRYO CHANGE
            output.close();//KRYO CHANGE
            byte[] dataToSend = output.getBuffer(); //KRYO CHANGE


            sendCopyToPubSocket(dataToSend);

        }else {
            System.out.println("[ZeroMQProducerClient]: Stable marker has been found, will NOT be forwarded to the warmup port....");
        }
    }


    private void sendCopyToPubSocket(byte[] incomingPayloadByteArray){
        int numberOfTuplesToSkip = 1;
        if(sendCounter == numberOfTuplesToSkip) {
            pub.send(incomingPayloadByteArray,ZMQ.NOBLOCK); //send to the warmup socket
            sendCounter = 0;
        }else{
            sendCounter++;
        }
    }


    protected void writeToZeroMqConsumer(Tuple tupleToSend, String tupleID, String tupleInternalID, String producerId, String timeStampFromSource, String operatorID, String tupleOrigin, String inputKey) {

        try {

            /**Zero MQ doesn't have a message data structure so we need to add a
             * {@link com.edgestream.worker.common.TupleHeader} to the {@link Tuple}
             * before putting it on the wire. TODO: change this to StreamElement
             * */

            TupleHeader tupleHeader = new TupleHeader(tupleID, timeStampFromSource, ZonedDateTime.now().toString(), operatorID, tupleOrigin, producerId);
            tupleToSend.setTupleHeader(tupleHeader);

            //byte[] dataToSend = SerializationUtils.serialize(tupleToSend);
            Output output = new Output(1024, -1);  //KRYO CHANGE
            kryo.writeObject(output, tupleToSend); //KRYO CHANGE
            output.close();//KRYO CHANGE
            byte[] dataToSend = output.getBuffer(); //KRYO CHANGE


            sendToZeroMQConsumers(dataToSend);


            //don't send stable markers to the warmup socket
            if(!tupleToSend.isStableMarker()) {
                sendCopyToPubSocket(dataToSend);
            }else{
                System.out.println("[ZeroMQProducerClient]: Stable marker has been found, will NOT be forwarded to the warmup port....");
            }



            if(!tupleToSend.isStableMarker() && !tupleToSend.isReconfigMarker())  {
                if(operatorIsInWarmUpPhase){
                    tupleToSend.setToWarmUpTuple();
                }


                //this.containerOutputEventWriter.sendOutputEvent(new OutputEventMetric(ZonedDateTime.now().toString(),tupleInternalID, Long.valueOf(dataToSend.length), inputKey, timeStampFromSource, tupleToSend.isLiveTuple()));

                //TODO: to be replace by a call to the ZMQContainerMetricsService
                //this.metricsCollector.AddOutputEventSize(tupleInternalID, Long.valueOf(dataToSend.length), inputKey, timeStampFromSource, tupleToSend.isLiveTuple());
            }
        } catch (Exception e) {
            e.printStackTrace();

        }
    }


    private void sendToZeroMQConsumers(byte[] dataToSend) {

        try {
            //System.out.println("Sending tuples to workers\n");
            //  The first message is "0" and signals start of batch
            //sender.send("0", 0);
            sender.send(dataToSend);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }


}




