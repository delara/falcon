package com.edgestream.worker.testing.Application;

import com.edgestream.worker.common.Tuple;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.client.*;
import org.apache.commons.lang3.SerializationUtils;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;



public class TestSinkDriver {

    public static void main(final String[] args) throws Exception {



/*        int number_of_consumers = Integer.parseInt(args[0]);
        int client_buffer_size = Integer.parseInt(args[1]);
        long desired_runtime = Long.parseLong(args[2]);
        int desired_batch_size = Integer.parseInt(args[3]);
        int desired_consumer_maxRate = Integer.parseInt(args[4]);*/

        String logpath = "C:\\bench\\artemis.consumer.bridged.exp.log.";
        //String logpath = "/home/ubuntu/logs/artemis.consumer.bridged.exp.log.";

        int desired_consumer_maxRate = 100000; // not used

/*        int number_of_consumers = 5;
        long desired_runtime = 100;
        int batchCommitSize = 100;*/

        int number_of_consumers = Integer.parseInt(args[0]);
        long desired_runtime = Long.parseLong(args[1]);
        int batchCommitSize = Integer.parseInt(args[2]);
        int client_buffer_size = Integer.parseInt(args[3]);



        System.out.println("Will create [" +  number_of_consumers +  "] consumers1");

        for (int i=1; i <= number_of_consumers;i++){

            new Sink("Consumer_" + i, client_buffer_size, desired_runtime, batchCommitSize,desired_consumer_maxRate, "input_queue_" + i,logpath).start();
        }


    }
}


class Sink extends Thread {

    ClientConsumer consumer1;
    ClientConsumer consumer2;
    ClientSession session1;
    ClientSession session2;
    long experimentStartTime;
    long runningTotal =0l;
    String consumerName;
    long desired_runtime;



    public Sink(String name, int client_buffer_size, long desired_runtime, int batchCommitSize , int desired_consumer_maxRate, String inputQueueName, String logpath) throws Exception {


        this.consumerName = name;
        this.desired_runtime =  desired_runtime;


        System.out.println("Starting consumer: " + this.consumerName);
        System.out.println("Client buffer size set to: " + client_buffer_size);
        //System.out.println("Consumer Max Rate is set to: " + desired_consumer_maxRate);

        //ServerLocator locator = ActiveMQClient.createServerLocator("tcp://localhost:61616");
        ServerLocator locator1 = ActiveMQClient.createServerLocator("tcp://10.70.2.224:61616");
        //ServerLocator locator2 = ActiveMQClient.createServerLocator("tcp://10.70.20.111:61616");
        //locator.setConsumerMaxRate(desired_consumer_maxRate);

        //locator.setConfirmationWindowSize(2000000);
        locator1.setConsumerWindowSize(client_buffer_size);
        //locator2.setConsumerWindowSize(client_buffer_size);

        ClientSessionFactory factory1 =  locator1.createSessionFactory();
        //ClientSessionFactory factory2 =  locator2.createSessionFactory();





        session1 = factory1.createSession();
        //session2 = factory1.createSession();



        try {
            session1.start();
            //session2.start();
        } catch (ActiveMQException e) {
            e.printStackTrace();
        }


        System.out.println("Connected to Artemis Broker");

        //String queueAddress  = "input-queue";




        String FQQN = "merlin_default::" + "merlin_default_to_be_forwarded_to_parent";
        //String FQQN2 = "experiments::" + "input-queue";





        try {
            consumer1 = session1.createConsumer(FQQN);
            //consumer2 = session2.createConsumer(FQQN);

        } catch (ActiveMQException e) {
            e.printStackTrace();
        }
        try {
            consumer1.setMessageHandler(new MyMessageHandler(this.consumerName + "_1", this.consumerName +"_handler_1" , batchCommitSize, this.session1, logpath));
            //consumer2.setMessageHandler(new MyMessageHandler(this.consumerName + "_2", this.consumerName +"_handler_2", batchCommitSize,this.session2,logpath));
        } catch (ActiveMQException e) {
            e.printStackTrace();
        }

        experimentStartTime = System.nanoTime();




    }


    class MyMessageHandler implements MessageHandler {


        String filename;
        PrintWriter writer;
        String consumerName;

        // Statistics
        long currentRecordcount = 0l;
        long windowStartTime  = System.nanoTime();
        int batch_size;
        ClientSession localSession;

        Kryo kryo;

        public MyMessageHandler (String consumerName, String handlerName, int batch_size_input , ClientSession session, String logPathDirectory) throws FileNotFoundException {
            this.localSession = session;

            this.consumerName = consumerName;

            this.batch_size = batch_size_input;
            System.out.println("Creating handler: " + handlerName);

            //filename =  "/home/ubuntu/artemis.consumer.bridged.exp.log." + this.consumerName;
            filename =  logPathDirectory + this.consumerName;
            writer  = new PrintWriter(new FileOutputStream(new File(filename), false));

            kryo = new Kryo();
            kryo.register(SimpleTuple.class);

        }



        /////////////////////////////////

        int counter;

        int sequenceCounter = 0;
        double windowMedianLatency =0;
        long window_total_latency =0;

        ArrayList<Long> window_latencies = new ArrayList<>();


        @Override
        public void onMessage(ClientMessage clientMessage) {


  /*          ByteArrayOutputStream bos = new ByteArrayOutputStream();
            try {
                clientMessage.saveToOutputStream(bos);
            } catch (ActiveMQException e) {
                e.printStackTrace();
            }

            byte[] incomingPayloadByteArray = bos.toByteArray();

            Tuple tuple = SerializationUtils.deserialize(incomingPayloadByteArray);
            System.out.println("After: "+  tuple.getPayload());*/

            String payload = clientMessage.getStringProperty("name");
            System.out.println(payload);

            byte [] incomingPayload = clientMessage.getBytesProperty("payload");

            int offset= clientMessage.getIntProperty("offset");
            int position = clientMessage.getIntProperty("position");

            Input input = new Input(incomingPayload, offset, position);
            SimpleTuple object2 = kryo.readObject(input, SimpleTuple.class);

            System.out.println("From kryo: [" + object2.payload + "]");



            counter++;
            runningTotal++;

            long new_message_produced_time = clientMessage.getTimestamp();
            long new_message_latency = System.currentTimeMillis() - new_message_produced_time;

            //System.out.println("Message Latency: " +new_message_latency);

            window_latencies.add(new_message_latency);
            window_total_latency = window_total_latency + new_message_latency;

            try {
                clientMessage.acknowledge();
            } catch (ActiveMQException e) {
                    e.printStackTrace();
            }

            if (counter > batch_size){
                counter = 0;
                try {
                    this.localSession.commit();

                    /*****************Log the throughput******************************/
                    long windowEndTime = System.nanoTime();
                    long windowDuration = windowEndTime - windowStartTime;

                    if (windowDuration > 1000000000l && currentRecordcount!=0) {


                        //windowMedianLatency = computeWindowLatencyMedian(window_latencies);
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

                } catch (ActiveMQException e) {
                    e.printStackTrace();
                }
            }

        }

        double computeWindowLatencyMedian(ArrayList<Long> listOfLatencies){



            long[] temp_list = new long[listOfLatencies.size()];

            int i =0;
            for (Long latencyReading : listOfLatencies){

                temp_list[i] = latencyReading;
                i++;
            }

            Arrays.sort(temp_list);
            double median;
            if (temp_list.length % 2 == 0)
                median = ((double)temp_list[temp_list.length/2] + (double)temp_list[temp_list.length/2 - 1])/2;
            else
                median = (double) temp_list[temp_list.length/2];
            return median;
        }
    }


    public void run(){
        try {
            Thread.sleep(60000 * this.desired_runtime); // experiment will run for n minutes

            session1.close();
            session2.close();


            long experimentEndTime = System.nanoTime();

            long experimentRunTime = experimentEndTime - experimentStartTime;

            System.out.println(this.consumerName  + " Experiment Run time: " + experimentRunTime / 1000000000l);

            System.out.println(this.consumerName  + " Throughput tuples/sec: " + runningTotal / (experimentRunTime / 1000000000l));




        } catch (InterruptedException | ActiveMQException e) {
            e.printStackTrace();
        }

    }




}
