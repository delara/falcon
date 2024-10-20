package com.edgestream.worker.io.zeroMQ;

import com.edgestream.worker.common.IDGenerator;
import com.edgestream.worker.common.Tuple;
import com.edgestream.worker.common.TupleHeader;
import com.edgestream.worker.io.MessageConsumerClient;
import com.edgestream.worker.io.TupleConsumer;
import com.edgestream.worker.metrics.common.InputEventMetric;
import com.edgestream.worker.metrics.metricscollector2.MetricsCollector2;
import com.edgestream.worker.metrics.metricscollector2.MetricsCollector3;
import com.edgestream.worker.metrics.metricscollector2.io.ContainerInputEventWriter;
import com.edgestream.worker.runtime.task.model.Task;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import org.apache.commons.lang3.SerializationUtils;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;


public class ZeroMQConsumerClient extends MessageConsumerClient {


    /*****************************************************************************************************
     * A ZeroMQConsumerClient is a server that services {@link ZeroMQProducerClient}
     * and sends data to an  {@link com.edgestream.worker.operator.Operator}. It may receive data from
     * other {@link ZeroMQProducerClient} or {@link com.edgestream.worker.io.artemis.ActiveMQToZeroMQMessageConsumerClientV2}
     ************************************************************************************************/

    private final String ConsumerClientID;
    private final ZeroMQConsumer primaryConsumer;
    private MetricsCollector3 metricsCollector;



    public ZeroMQConsumerClient(String ConsumerClientID, Task taskToRun, String serviceIP, String servicePort, String inputMethod, String predecessorIP, String predecessorPort, String metricsInputEventPortNumber) {

        this.metricsCollector = metricsCollector;
        this.ConsumerClientID = ConsumerClientID;
        this.boundTask = taskToRun;
        this.primaryConsumer = new ZeroMQConsumer(this.boundTask, this.ConsumerClientID, serviceIP, servicePort, inputMethod, predecessorIP, predecessorPort, metricsInputEventPortNumber);
        this.primaryConsumer.setName("ZeroMQConsumer");


    }


    @Override
    public void startTask() {

        this.primaryConsumer.start();
    }


    /**********************************************************
     *                  Warm up methods
     *
     *
     ************************************************************/

    public void resetBoundTask(){

        this.primaryConsumer.getBoundTask().resetBoundOperator();

    }

    public void enableBoundTaskWarmUp(){

        this.primaryConsumer.getBoundTask().enableBoundOperatorWarmupPhase();
    }

    public void disableBoundTaskWarmUp(){

        this.primaryConsumer.getBoundTask().disableBoundOperatorWarmupPhase();
    }

    @Override
    public void setOperatorIsInWarmUpPhase(boolean inWarmUpPhase) {
        this.primaryConsumer.notifyThatOperatorIsInWarmUpPhase(inWarmUpPhase);
    }

    /***************************************************************************
     *  This was moved from the ZeroMQConsumer constructor to allow for the warmup phase to run before consuming live tuples.
     *  This should be called after the warm up phase. It tells this consumer to connect the its predecessor ZMQ port
     ***************************************************************************/
    public void  connectToPredecessor(){
        this.primaryConsumer.connectToPredecessor();
    }








    private class ZeroMQConsumer extends Thread implements TupleConsumer {

        private final String consumerName;
        private final Task boundTask;
        private final String serviceIP;
        private final String servicePort;
        private ZContext context;
        private ZMQ.Socket receiver; // THIS RECEIVER IS BOUND TO 2 ports, port 7000 to get tuples from the AMQ consumer client and port 7002 to get tuples from a predecessor that came online after this operator was started

        private boolean operatorIsInWarmUpPhase = true;

        //ID generator
        private IDGenerator generator;

        //warm up zmq configs
        private ZMQConsumerDummyTupleReceiver ZMQconsumerDummyTupleReceiver;
        private int sendCounter= 0;
        private ZMQ.Socket pub;


        private final String predecessorInputMethod;
        private final String predecessorIP;
        private final String predecessorPort;


        private final ContainerInputEventWriter containerInputEventWriter;
        private final Kryo kryo = new Kryo();

        private final int HWM =50;


        private long received_counter = 0l;

        public ZeroMQConsumer(
                Task boundTask
                , String name
                , String serviceIP
                , String servicePort
                , String predecessorInputMethod
                , String predecessorIP
                , String predecessorPort
                , String metricsInputEventPortNumber
        ) {

            this.boundTask = boundTask;
            this.consumerName = name;
            this.serviceIP = serviceIP;
            this.servicePort = servicePort;
            this.predecessorInputMethod = predecessorInputMethod;
            this.predecessorIP = predecessorIP;
            this.predecessorPort =predecessorPort;
            this.containerInputEventWriter = new ContainerInputEventWriter(metricsInputEventPortNumber);


            this.setGenerator(new IDGenerator());

            System.out.println("[ZeroMQConsumerClient]: Starting Zero MQ consumer: " + this.consumerName);
            printWarmUpState();

            /**
             * Here we create ONE receiver that can connect to multiple producers
             * 1. We consume from the ActiveMQConsumer that is receiving data from the broker and writing it to port 7000
             * 2. At start time: If there has been a candidate predecessor identified by the Worker, the Worker will set the predecessorInputMethod to zeromq,
             * so in this case we create a second connection to read from that external container.
             *
             * */
            try {
                this.context = new ZContext();
                //  Socket to receive messages on from ActiveMQ local producer
                this.receiver = context.createSocket(SocketType.PULL);
                this.receiver.setReceiveTimeOut(5000);
                this.receiver.setHWM(HWM);

                String spoutInputPort = "6999";
                this.receiver.bind("tcp://*:" + spoutInputPort);


                String lateProducerPort = "7002";
                this.receiver.bind("tcp://" + this.serviceIP + ":" + lateProducerPort);
                // port to receive warmup tuples
                String warmerTuplePort = "7003";
                this.receiver.bind("tcp://" + this.serviceIP + ":" + warmerTuplePort);

                //We bind the Receiver to 7002 so that we can receive data from predecessor operators
                //that were created after this operator existed. That predecessor will reach out and
                // make the connection to this port when they come online
                this.receiver.connect("tcp://" + this.serviceIP + ":" + this.servicePort); //always 7000

                //This socket will publish the input received by this container to the dummy INPUT warmer socket listening on port 9000
                System.out.println("[ZeroMQConsumerClient]: Creating pub socket on port [9000]");
                this.pub = context.createSocket(SocketType.PUB);
                this.pub.bind("tcp://localhost:9000");

                //this socket will receive the messages published to port 9000. The purpose of
                // creating this dummy receiver,is to keep the pub socket warm
                System.out.println("[ZeroMQConsumerClient]: Creating dummy pub receiver...."); //this just receives a copy of the tuple and drops it to keep the socket warm
                this.ZMQconsumerDummyTupleReceiver = new ZMQConsumerDummyTupleReceiver(this.context,"ZMQ_CONSUMER_DUMMY_PR_1");
                this.ZMQconsumerDummyTupleReceiver.start();

            } catch (Exception e) {
                e.printStackTrace();
            }

            kryo.register(TupleHeader.class);
            kryo.register(byte[].class);
            kryo.register(Tuple.class);

        }


        public void connectToPredecessor(){

            // Socket used at start time to connect to existing ZEROMQ producers
            // TODO: this will need to be updated to delay the connection to the predecessor if we need to warm first before connecting to the predecessor
            if (predecessorInputMethod.equalsIgnoreCase("zeromq")) {
                System.out.print("[ZeroMQConsumerClient]: There is a valid predecessor online and this container will connect to it with the following configuration: ");
                System.out.println("predecessorIP[" + predecessorIP +"]  predecessorPort  [" +  predecessorPort + "]" );
                //  Socket to receive messages from remote zeroMQ producer
                receiver.connect("tcp://" + predecessorIP + ":" + predecessorPort);
            }else{
                System.out.println("[ZeroMQConsumerClient]: NO predecessor is online, this operator is getting its tuples from the broker ");
            }
        }


        @Override
        public void notifyThatOperatorIsInWarmUpPhase(boolean inWarmUpPhase) {
            operatorIsInWarmUpPhase = inWarmUpPhase;
            printWarmUpState();
        }

        @Override
        public void printWarmUpState() {
            System.out.println("[ZeroMQConsumerClient]: Operator warm up state is: ["+operatorIsInWarmUpPhase+"]");
        }

        private void sendCopyToPubSocket(byte[] incomingPayloadByteArray){
            int numberOfTuplesToSkip = 1;
            if(sendCounter == numberOfTuplesToSkip) {
                this.pub.send(incomingPayloadByteArray,ZMQ.NOBLOCK); //for the warmup socket
                sendCounter = 0;
            }else{
                sendCounter++;
            }
        }



        private void receiveTuplesAsync() throws Exception {
            //  Process tuples forever
            System.out.println("[ZeroMQConsumerClient]: Starting Receive Thread");

            //while (!Thread.currentThread().isInterrupted()) {
            while (true) {

                byte[] incomingPayloadByteArray = receiver.recv();

                if (incomingPayloadByteArray != null) {

                    //System.out.println("[ZeroMQConsumerClient]: I received a tuple, sending a copy to the push socket");

                    received_counter++;
                    //System.out.println("ZeroMQConsumerClient: received this many tuples [" +received_counter+"]"); // FOR DEBUG ONLY

                    //System.out.println("[ZeroMQConsumerClient]: I received a tuple, sending it to the operator");
                   //Tuple tuple = SerializationUtils.deserialize(incomingPayloadByteArray); //KRYO CHANGE

                    Input input = new Input(incomingPayloadByteArray); //KRYO CHANGE
                    Tuple tuple = kryo.readObject(input, Tuple.class); //KRYO CHANGE


                    String transfer = tuple.getTupleHeader().getTimestampTransfer();
                    String previous_operator = tuple.getTupleHeader().getPreviousOperator();
                    String timeStampFromSource = tuple.getTupleHeader().getTimeStampFromSource();
                    String tupleID = tuple.getTupleHeader().getTupleID();
                    String key = tuple.getKeyValue();
                    String tupleInternalID = IDGenerator.generateType1UUID().toString();
                    String tupleOrigin = tuple.getTupleHeader().getTupleOrigin();
                    String producerId = tuple.getTupleHeader().getProducerId();

                    if (tuple.isReconfigMarker()) {
                        System.out.println("[ZeroMQConsumerClient] Received reconfig marker");
                    }

                    if (!tuple.isReconfigMarker() && !tuple.isStableMarker()) {
                        if(operatorIsInWarmUpPhase){
                            tuple.setToWarmUpTuple();
                        }
                        //TODO: to be replaced by a single call to the ZMQMetricsConsumerClient
                        //this.containerInputEventWriter.sendInputEvent(new InputEventMetric(tupleID,timeStampFromSource,Long.valueOf(incomingPayloadByteArray.length),previous_operator,transfer,key,tuple.isLiveTuple(),tupleInternalID,tupleOrigin));

//                        this.metricsCollector.AddInputEventSize(tupleInternalID, timeStampFromSource, Long.valueOf(incomingPayloadByteArray.length), previous_operator, transfer, key, tuple.isLiveTuple());
//                        this.metricsCollector.includeOriginId(tupleOrigin);


                    }
                    boundTask.processIncomingElement(tuple, tupleID, tupleInternalID, producerId, timeStampFromSource, tuple.getTupleHeader().getTupleOrigin(), Long.valueOf(incomingPayloadByteArray.length), key);
                    sendCopyToPubSocket(incomingPayloadByteArray);

                }else{
                    System.out.println("[ZeroMQConsumerClient]: I did not receive a live tuple in the last 5 seconds");
                }
            }
        }


        Task getBoundTask(){

           return boundTask;

        }


        public void run() {

            try {
                receiveTuplesAsync();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        public IDGenerator getGenerator() {
            return generator;
        }

        public void setGenerator(IDGenerator generator) {
            this.generator = generator;
        }


    }

    private class ZMQConsumerDummyTupleReceiver implements Runnable{

        private final ZMQ.Socket subSocket;
        private Thread t;
        private final String tName;
        private int counter =0;

        public ZMQConsumerDummyTupleReceiver(ZContext context, String tName) {
            this.tName = tName;
            subSocket = context.createSocket(SocketType.SUB);
            subSocket.subscribe("".getBytes());
            subSocket.setReceiveTimeOut(5000);
            subSocket.connect("tcp://localhost:9000");

        }

        @Override
        public void run() {
            System.out.println("[ZMQConsumerDummyTupleReceiver]: Receiver thread started");
            //while(!Thread.interrupted()) {

            while(true) {
                byte[] incomingPayloadByteArray = subSocket.recv(0);

                if(incomingPayloadByteArray !=null) {
                    //System.out.println("[ZMQConsumerDummyTupleReceiver]: I received a warm up tuple from my consumer!"); //for debug
                    //printTupleDetails(incomingPayloadByteArray);
                    counter++;
                }else{
                    System.out.println("[ZMQConsumerDummyTupleReceiver]: I did not receive a warmup tuple from my consumer in the last 5 seconds");
                    System.out.println("[ZMQConsumerDummyTupleReceiver]: Warmup Tuples received since the start: [" + counter + "]");
                }


            }
        }

        public void start() {
            System.out.println("[ZMQConsumerDummyTupleReceiver]: Starting " +  this.tName );
            if (t == null) {
                t = new Thread (this, tName);
                t.start();
            }
        }

        private void printTupleDetails(byte[] tupleArray){
            Tuple tuple = SerializationUtils.deserialize(tupleArray);


            System.out.println("[ZMQConsumerDummyTupleReceiver]: Tuple details " + "ID: " + tuple.getTupleHeader().getTupleID() + "Type: "+ tuple.getType());

        }
    }
}
