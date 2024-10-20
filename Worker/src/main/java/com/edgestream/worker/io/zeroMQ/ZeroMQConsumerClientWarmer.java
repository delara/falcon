package com.edgestream.worker.io.zeroMQ;


import com.edgestream.worker.common.Tuple;
import org.apache.commons.lang3.SerializationUtils;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;


public class ZeroMQConsumerClientWarmer {

    private String consumerName;
    private ZContext context;
    private ZMQ.Socket warmerReceiver;
    private ZMQ.Socket pusher;
    private int warmUpTupleReceivedCount = 0;
    private final long beginTime;
    private final long timeToWarmMilliseconds = 1000 * 60; // seconds


    public ZeroMQConsumerClientWarmer(String ConsumerClientID, String warmupSourcePort, String warmupSourceIP) {

        System.out.println("[ZeroMQConsumerClientWarmer]: Starting warmer with the following configs: ConsumerClientID[" +  ConsumerClientID +"]" +" | warmupSourcePort(Should be 9000 if same type or 9001 if predecessor container will be used. WARMER CONFIG {warmupSourcePort[" +warmupSourcePort +" | ConsumerClientID[" + warmupSourceIP +"]}");
        this.beginTime = System.currentTimeMillis();
        startWarmer(ConsumerClientID, warmupSourcePort, warmupSourceIP);
    }


    private void startWarmer(String ConsumerClientID, String warmupSourcePort, String warmupSourceIP) {
        this.consumerName = ConsumerClientID;
        System.out.println("[ZeroMQConsumerClientWarmer]: Starting Zero MQ consumer warmer: " + this.consumerName);
        try {
            this.context = new ZContext();
            //  Socket to consume messages from the source container
            createPushSocket();
            warmerReceiver= context.createSocket(SocketType.SUB);
            warmerReceiver.subscribe("".getBytes());
            warmerReceiver.setReceiveTimeOut(1000);
            warmerReceiver.connect("tcp://" + warmupSourceIP + ":" + warmupSourcePort);

            //createPushSocket();

        } catch (Exception e) {
            e.printStackTrace();
        }


        System.out.println("[ZeroMQConsumerClientWarmer]: Will warm for 60 seconds....");
        receiveTuples();

        System.out.println("[ZeroMQConsumerClientWarmer]: Stopping the warmer.....waiting 10 seconds to clear tuples inside the operator");
        try {
            Thread.sleep(1000 * 10);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        System.out.println("[ZeroMQConsumerClientWarmer]: Total warm up tuples received during warm up phase: [" + warmUpTupleReceivedCount + "]");
    }


    private void createPushSocket() {
        String operatorWarmUpSocket = "7003";
        try {
            //The operator inside this container listens on many ports and port 7003 is a port that the operator will receive warm up tuples so we need to connect to that socket to PUSH warm up tuples to it
            String warmerPort = "7003";
            pusher = context.createSocket(SocketType.PUSH);
            pusher.connect("tcp://" + "localhost" + ":" + warmerPort);
            System.out.println("[ZeroMQConsumerClientWarmer]:  ZeroMQ Push Socket Created, will push warm up tuple to port ["+ operatorWarmUpSocket +"]");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    private void printTupleDetails(byte[] tupleArray){
        Tuple tuple = SerializationUtils.deserialize(tupleArray);

        System.out.println("[ZeroMQConsumerClientWarmer]: Received tuple count [" + warmUpTupleReceivedCount + "]");
        System.out.println("[ZeroMQConsumerClientWarmer]: Tuple details " + "ID: " + tuple.getTupleHeader().getTupleID() + "Type: "+ tuple.getType());

    }


    private void receiveTuples() {
        System.out.println("[ZeroMQConsumerClientWarmer]: Starting warmup consumer and forward phase...");
        boolean timerExpired = false;
        int no_message_counter = 0;
        //int yes_message_counter = 0;
        while (!timerExpired) {

            byte[] incomingPayloadByteArray = warmerReceiver.recv(0);
            if(incomingPayloadByteArray != null){
                warmUpTupleReceivedCount++;
                //System.out.println("[ZeroMQConsumerClientWarmer]: Attempting to send tuple to ZeroMqConsumerClient...");
                pusher.send(incomingPayloadByteArray);
                //System.out.println("[ZeroMQConsumerClientWarmer]: Sent tuple to ZeroMqConsumerClient...");
                //printTupleDetails(incomingPayloadByteArray);


            }else{
                no_message_counter++;
                //System.out.println("[ZeroMQConsumerClientWarmer]: Counter ran this many times: ["+ no_message_counter+ "] No warm up messages received from predecessor container yet");
            }

            if (warmPeriodFinished()) {
                System.out.println("[ZeroMQConsumerClientWarmer]: Warm up is over..");
                System.out.println("[ZeroMQConsumerClientWarmer]: I received this many warm up tuples..[" + warmUpTupleReceivedCount + "]");

                timerExpired = true;
                warmerReceiver.close();
                pusher.close();
            }

        }
    }

    private boolean warmPeriodFinished(){
        boolean warmPeriodFinished = false;

        long timeSinceLaunch = System.currentTimeMillis() - beginTime;
        if(timeSinceLaunch > timeToWarmMilliseconds){
            warmPeriodFinished = true;
        }

        return warmPeriodFinished;
    }
}





