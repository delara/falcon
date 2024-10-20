package com.edgestream.worker.io.artemis;


import com.edgestream.worker.common.Tuple;
import com.edgestream.worker.common.TupleHeader;
import com.edgestream.worker.io.MessageConsumerClient;
import com.edgestream.worker.io.zeroMQ.ZeroMQConsumerClient;
import com.edgestream.worker.io.zeroMQ.ZeroMQConsumerClientWarmer;
import com.edgestream.worker.metrics.metricscollector2.MetricsCollector2;
import com.edgestream.worker.metrics.metricscollector2.MetricsCollector3;
import com.edgestream.worker.runtime.task.model.Task;
import com.edgestream.worker.runtime.task.model.TaskWarmerStrategy;
import com.edgestream.worker.testing.Application.SimpleTuple;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.client.*;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import java.io.ByteArrayOutputStream;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;


public class ActiveMQToZeroMQSwitchableMessageConsumerClient extends MessageConsumerClient {


    /*****************************************************************
     * A client that is bound to a queue that will receive a
     * {@link org.apache.activemq.artemis.core.message.impl.CoreMessage}
     * and pass it to a ZeroMQ socket     *
     *****************************************************************/

    private final String ConsumerClientID;
    private final String FQQN;
    private ArtemisToZeroMQConsumer artemisToZeroMQConsumer;
    private final ZeroMQConsumerClient zeroMQConsumerClient;

    private final String warmupContainerIP;
    private final int client_buffer_size;
    private final int desired_batch_size;
    private final String brokerIP;
    private static final String WARM_FROM_INPUT_PORT = "9000";
    private static final String WARM_FROM_OUTPUT_PORT = "9001";




    public ActiveMQToZeroMQSwitchableMessageConsumerClient(String ConsumerClientID, String FQQN, Task taskToRun, String brokerIP
            , int batchSize, int bufferSize, String inputMethod, String predecessorIP, String predecessorPort
            , TaskWarmerStrategy warmupFlag, String warmupContainerIP, String metricsInputEventPortNumber)  {

        this.ConsumerClientID = ConsumerClientID;
        this.FQQN = FQQN;
        this.boundTask = taskToRun;
        setTaskWarmerStrategy(warmupFlag);
        this.warmupContainerIP = warmupContainerIP;
        this.brokerIP = brokerIP;
        this.client_buffer_size = bufferSize;
        this.desired_batch_size = batchSize;


        System.out.println("[ActiveMQToZeroMQSwitchableMessageConsumerClient]:  Creating new instance");

        //Start the ZeroMQ consumer client thread that will read from this thread
        String ZeroMQConsumerClientID = ConsumerClientID + "_ZeroMQ";
        String ZeroMQServiceIP = "localhost";
        String ZeroMQServicePort = "7000";
        this.zeroMQConsumerClient = new ZeroMQConsumerClient(ZeroMQConsumerClientID, taskToRun, ZeroMQServiceIP, ZeroMQServicePort
                , inputMethod, predecessorIP, predecessorPort,metricsInputEventPortNumber);

        this.zeroMQConsumerClient.startTask();

        try {
            //artemisToZeroMQConsumer = new ArtemisToZeroMQConsumer(this.ConsumerClientID, client_buffer_size, desired_batch_size, this.FQQN, brokerIP, metricsCollector);
            artemisToZeroMQConsumer = new ArtemisToZeroMQConsumer(this.ConsumerClientID, client_buffer_size, desired_batch_size, this.FQQN, brokerIP);
        } catch (Exception e) {
            e.printStackTrace();
        }

        System.out.println("[ActiveMQToZeroMQSwitchableMessageConsumerClient]: Using Switchable AMQ + ZMQ MCC");

    }

    @Override
    public void setOperatorIsInWarmUpPhase(boolean inWarmUpPhase) {
        //The ZMQ consumer is the one that calls the metrics collector so we need to notify it about the operator state
        this.zeroMQConsumerClient.setOperatorIsInWarmUpPhase(inWarmUpPhase);
    }

    @Override
    public void startTask() {

        if(getTaskWarmerStrategy() == TaskWarmerStrategy.YES_SAME_CONTAINER_TYPE_SWITCH_AMQ
            || getTaskWarmerStrategy() == TaskWarmerStrategy.YES_SAME_CONTAINER_TYPE_SWITCH_ZMQ
            || getTaskWarmerStrategy() == TaskWarmerStrategy.YES_PREDECESSOR_CONTAINER){

            System.out.println("[ActiveMQToZeroMQSwitchableMessageConsumerClient]: Operator needs to be warmed up...");
            String inputPort = null;

            if (getTaskWarmerStrategy() == TaskWarmerStrategy.YES_SAME_CONTAINER_TYPE_SWITCH_AMQ || getTaskWarmerStrategy() == TaskWarmerStrategy.YES_SAME_CONTAINER_TYPE_SWITCH_ZMQ){
                inputPort = WARM_FROM_INPUT_PORT;
                System.out.println("[ActiveMQToZeroMQSwitchableMessageConsumerClient]: Setting warm from port INPUT: " + inputPort);
            }

            if (getTaskWarmerStrategy() == TaskWarmerStrategy.YES_PREDECESSOR_CONTAINER){
                inputPort = WARM_FROM_OUTPUT_PORT;
                System.out.println("[ActiveMQToZeroMQSwitchableMessageConsumerClient]: Setting warm from port OUTPUT: " + inputPort);
            }

            //a.  Attach the warmer to replica container and start it for 60 seconds
            new ZeroMQConsumerClientWarmer(this.ConsumerClientID+ "_warmer",inputPort,this.warmupContainerIP);


            //d. Call reset function on the operator
            this.zeroMQConsumerClient.resetBoundTask();
            //e. attach ActiveMQ consumer from

            this.zeroMQConsumerClient.disableBoundTaskWarmUp();
        }

        //Start the real consumer
        System.out.println("[ActiveMQToZeroMQSwitchableMessageConsumerClient]: Starting activemq consumer");
        startArtemisToZeroMQConsumer();
    }


    private void startArtemisToZeroMQConsumer(){

        artemisToZeroMQConsumer.startConsumerSession();
        zeroMQConsumerClient.connectToPredecessor();

    }



    class ArtemisToZeroMQConsumer extends Thread {
        private ClientConsumer consumer1;
        private final ClientSession session;
        private final String consumerName;

        //Control system
        private final AtomicInteger commitCounter = new AtomicInteger(0);
        private ZonedDateTime lastReceiving;
        private final AtomicBoolean running = new AtomicBoolean(false);
        private final AtomicBoolean closed = new AtomicBoolean(false);
        final int LIMIT_TIME = 5000;
        private int batchSize;


        //public ArtemisToZeroMQConsumer(String name, int client_buffer_size, int desired_batch_size, String FQQN, String brokerIP, MetricsCollector2 metricsCollector) throws Exception {
        public ArtemisToZeroMQConsumer(String name, int client_buffer_size, int desired_batch_size, String FQQN, String brokerIP) throws Exception {
            this.consumerName = name;
            this.setBatchSize(desired_batch_size);


            System.out.println("[ActiveMQToZeroMQSwitchableMessageConsumerClient]: Starting consumer: " + this.consumerName);
            System.out.println("[ActiveMQToZeroMQSwitchableMessageConsumerClient]: Client buffer size set to: " + client_buffer_size);


            ServerLocator locator = ActiveMQClient.createServerLocator(brokerIP);
            locator.setBlockOnDurableSend(false);
            locator.setBlockOnNonDurableSend(false);
            locator.setBlockOnAcknowledge(false);
            locator.setConsumerWindowSize(-1);


            ClientSessionFactory factory = locator.createSessionFactory();

            session = factory.createSession();
            System.out.println("[ActiveMQToZeroMQSwitchableMessageConsumerClient]: Connected to Artemis Broker");


            //try to connect to the queue
            try {
                consumer1 = session.createConsumer(FQQN);


            } catch (ActiveMQException e) {
                e.printStackTrace();
            }
            try {
                //consumer1.setMessageHandler(new MyMessageHandler(this.consumerName + "_1", this.consumerName + "_handler_1", metricsCollector));
                consumer1.setMessageHandler(new MyMessageHandler(this.consumerName + "_1", this.consumerName + "_handler_1"));

            } catch (ActiveMQException e) {
                e.printStackTrace();
            }



        }


        private void startConsumerSession(){
            try {
                session.start(); // The Thread starts here
            } catch (ActiveMQException e) {
                e.printStackTrace();
            }
        }


        /*****************************
         * MyMessageHandler is invoked when a message arrives. This happens asynchronously. The session is committed by the onMessage() method after the batch size count
         * threshold has been hit
         *******************************/
        class MyMessageHandler implements MessageHandler {
            ZContext context;
            String consumerName;
            //MetricsCollector2 metricsCollector;
            ZMQ.Socket sender;
            private final Kryo kryo = new Kryo(); //KRYO CHANGE
            private long received_counter =0l;

            //public MyMessageHandler(String consumerName, String handlerName, MetricsCollector2 metricsCollector) {
            public MyMessageHandler(String consumerName, String handlerName) {
                //this.metricsCollector = metricsCollector;
                this.consumerName = consumerName;
                this.context = new ZContext();
                createPushSocket();

                System.out.println("[ActiveMQToZeroMQSwitchableMessageConsumerClient]: Creating handler: " + handlerName);
                System.out.println("[ActiveMQToZeroMQSwitchableMessageConsumerClient]: Waiting for first message");
                System.out.println("******************************************************");

                kryo.register(TupleHeader.class); //KRYO CHANGE
                kryo.register(byte[].class); //KRYO CHANGE
                kryo.register(Tuple.class); //KRYO CHANGE
            }

            private void createPushSocket() {


                String localPort = "7000";
                try {

                    //  Socket to send messages on
                    sender = this.context.createSocket(SocketType.PUSH);
                    sender.bind("tcp://*:" + localPort);
                    System.out.println("[ActiveMQToZeroMQSwitchableMessageConsumerClient]: ZeroMQ Push Socket Created");

                } catch (Exception e) {
                    e.printStackTrace();

                }
            }




            /***
             * This is the method that receives the message. When the message is received we call the Task -> then the Operator to process the tuple.
             * @param clientMessage
             */
            @Override
            public void onMessage(ClientMessage clientMessage) {
                setLastReceiving(ZonedDateTime.now());

                /**1. ack and commit ******/
                try {
                    clientMessage.acknowledge();
                    session.commit();
                } catch (ActiveMQException e) {
                    e.printStackTrace();
                }


                /*ByteArrayOutputStream bos = new ByteArrayOutputStream();

                try {
                    clientMessage.saveToOutputStream(bos);

                } catch (ActiveMQException e) {
                    e.printStackTrace();
                }

                byte[] incomingPayloadByteArray = bos.toByteArray();


                 */
                byte [] incomingPayloadByteArray = clientMessage.getBytesProperty("payload"); //KRYO CHANGE
                received_counter++;
                System.out.println("ActiveMQToZeroMQMessageConsumerClientV2: received this many tuples [" +received_counter+"]");

                /**** Write to local ZeroMQ socket on the same machine/container*/
                sendToZeroMQConsumer(incomingPayloadByteArray);

                /************ActiveMQ session batch commits**************************************************************/
                commitCounter.getAndIncrement();

            }


            private void sendToZeroMQConsumer(byte[] dataToSend) {
                //  The first message is "0" and signals start of batch
                //sender.send("0", 0);
                sender.send(dataToSend);
            }


        }//end of message handler class




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
}
