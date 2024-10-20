package com.edgestream.worker.testing.Application;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;

import org.apache.commons.lang3.SerializationUtils;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.util.ArrayList;



public class ZMQTestSinkDriver {

    public static void main(final String[] args) throws Exception {

        //String logpath = "C:\\bench\\artemis.consumer.bridged.exp.log.";
        String logpath = "/home/ubuntu/logs/ZMQ.consumer.exp.log.";

        int number_of_consumers = Integer.parseInt(args[0]);
        String predecessorIP = args[1];
        String predecessorPort = args[2];
        String consumerName = "SinkConsumer";

        System.out.println("Will create [" +  number_of_consumers +  "] consumers1");



        for (int i=1; i <= number_of_consumers;i++){

            new ZMQSink(predecessorIP,predecessorPort,logpath,consumerName + i).start();
        }


    }
}


class ZMQSink extends Thread {

    long experimentStartTime;
    long runningTotal =0l;
    String consumerName;
    private ZContext context;
    private ZMQ.Socket receiver; // THIS RECEIVER IS BOUND TO 2 ports, port 7000 to get tuples from the AMQ consumer client and port 7002 to get tuples from a predecessor that came online after this operator was started
    String predecessorIP;
    String predecessorPort;
    Kryo kryo;


    String filename;
    PrintWriter writer;


    // Statistics
    long currentRecordcount = 0l;
    long windowStartTime  = System.nanoTime();
    int batch_size;




    int sequenceCounter = 0;
    double windowMedianLatency =0;
    long window_total_latency =0;

    ArrayList<Long> window_latencies = new ArrayList<>();



    public ZMQSink(String predecessorIP,String predecessorPort, String logPathDirectory, String consumerName) throws Exception {

        this.predecessorIP = predecessorIP;
        this.predecessorPort = predecessorPort;
        this.consumerName = consumerName;
        System.out.println("Starting consumer: " + this.consumerName);
        experimentStartTime = System.nanoTime();
        this.batch_size = 1;


        try {
            this.context = new ZContext();
            this.receiver = context.createSocket(SocketType.PULL);
            //this.receiver.setReceiveTimeOut(5000);
            //this.receiver.setConflate(true);
            //this.receiver.bind("tcp://" + this.predecessorIP + ":" + this.predecessorPort); //8000
            this.receiver.bind("tcp://broker4cpu-second:8000");
        }catch (Exception e) {
            e.printStackTrace();
        }


        connectToPredecessor();

        kryo = new Kryo();
        kryo.register(SimpleTuple.class);

        filename =  logPathDirectory + this.consumerName;
        writer  = new PrintWriter(new FileOutputStream(new File(filename), false));

    }


    public void connectToPredecessor(){
        receiver.connect("tcp://" + predecessorIP + ":" + predecessorPort);
    }



    public void updateMetrics(SimpleTuple simpleTuple){

        runningTotal++;

        long new_message_produced_time = simpleTuple.getCreateTime();
        long new_message_latency = System.currentTimeMillis() - new_message_produced_time;

        window_latencies.add(new_message_latency);
        window_total_latency = window_total_latency + new_message_latency;


        /*****************Log the throughput******************************/
        long windowEndTime = System.nanoTime();
        long windowDuration = windowEndTime - windowStartTime;

        if (windowDuration > 1000000000l && currentRecordcount!=0) {

            long windowAVGLatency = window_total_latency/currentRecordcount;

            writer.println("ConsumerID,SeqNumber,WindowDuration,Throughput,RunningTotal,MedianLatency,Total Latency,AVGLatency:" + this.consumerName  +" " + sequenceCounter  + " " + windowDuration +" " +  currentRecordcount +" " +  runningTotal + " " + windowMedianLatency + " "+ window_total_latency + " " + windowAVGLatency );
            System.out.println(this.consumerName + " received this many tuples in one second: "+ currentRecordcount + " The running total is: " + runningTotal + " Duration: " + windowDuration);
            //reset the counter
            currentRecordcount = 0 ;
            windowMedianLatency =0;
            window_latencies.clear();
            window_total_latency =0;

            windowStartTime = windowEndTime ;
            sequenceCounter++;
            writer.flush();
        }
        else{
            currentRecordcount = currentRecordcount + batch_size ; //keep incrementing until the next window because 1 second has not yet passed since the last window closed
        }

    }




    public void receiveTuplesAsync(){
        System.out.println("[ZeroMQConsumerClient]: Starting Receive Thread");

        //while (!Thread.currentThread().isInterrupted()) {
        while (true) {

            byte[] incomingPayloadByteArray = receiver.recv();

            if(incomingPayloadByteArray != null) {
                Input input = new Input(incomingPayloadByteArray);
                SimpleTuple simpleTuple = kryo.readObject(input, SimpleTuple.class);


                //SimpleTuple simpleTuple =SerializationUtils.deserialize(incomingPayloadByteArray);
                simpleTuple.getPayload();
                //System.out.println("From kryo: [" + simpleTuple.payload + "]");
                updateMetrics(simpleTuple);
            }
        }

    }


    public void run(){

        try {
            receiveTuplesAsync();
        } catch (Exception e) {
            e.printStackTrace();
        }


    }

}
