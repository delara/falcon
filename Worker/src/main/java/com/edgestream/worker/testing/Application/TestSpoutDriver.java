package com.edgestream.worker.testing.Application;

import com.edgestream.worker.common.Tuple;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.client.*;

import java.io.*;
import java.util.ArrayList;

public class TestSpoutDriver {



    public static void main(final String[] args) throws Exception {
        String topologyAddress = "merlin_default";
         // 2 100 1 10.70.20.111 merlin_default 200 100
/*      int number_of_producers = Integer.parseInt(args[0]);
        int tuple_payload_size  = Integer.parseInt(args[1]);
        long desired_runtime = Long.parseLong(args[2]);
        String broker_ip = args[3];
        String topologyAddress = args[4];

        int superTupleAmount =  Integer.parseInt(args[5]);
        int batchCommitSize  = Integer.parseInt(args[6]);*/



        int number_of_producers =  Integer.parseInt(args[0]);
        long desired_runtime =  Long.parseLong(args[1]);
        String broker_ip = args[2];
        int tuple_payload_size  = Integer.parseInt(args[3]);
        int superTupleAmount =  Integer.parseInt(args[4]);
        int batchCommitSize  = Integer.parseInt(args[5]);


/*        String topologyAddress = "merlin_default";
        int number_of_producers = 4;
        long desired_runtime = 1l;
        String broker_ip = "10.70.20.133";
        int tuple_payload_size  = 100;
        int superTupleAmount =  1;
        int batchCommitSize  = 100;*/



        for (int i=0; i < number_of_producers;i++){

            new Spout("Producer : " + i , i, tuple_payload_size, desired_runtime,broker_ip,topologyAddress,superTupleAmount,batchCommitSize).start();
        }


    }
}


class Spout extends Thread {

    ClientProducer producer1;
    ClientProducer producer2;
    ClientSessionFactory clientSessionFactory1;
    ClientSessionFactory clientSessionFactory2;

    ClientSession clientSession1;
    ClientSession clientSession2;




    Integer ProducerID;
    int tuple_payload_size_bytes;
    StringBuilder payload;
    ServerLocator locator1;
    ServerLocator locator2;
    long desired_runtime;
    String topologyAddress;
    int superTupleAmount;
    int batchCommitSize;
    ArrayList<Tuple> tupleBatch = new ArrayList<>();
    Kryo kryo;



    public Spout(String name, Integer ProducerID, int tuple_payload_size, long desired_runtime, String broker_ip , String topologyAddress, int superTupleAmount, int batchCommitSize) throws Exception {

        this.ProducerID = ProducerID;
        this.tuple_payload_size_bytes = tuple_payload_size;
        this.desired_runtime = desired_runtime;
        this.topologyAddress = topologyAddress;
        this.superTupleAmount = superTupleAmount;
        this.batchCommitSize = batchCommitSize;

        kryo = new Kryo();
        kryo.register(SimpleTuple.class);

        System.out.println("Starting producer: " + name);


        //this.locator = ActiveMQClient.createServerLocator("udp://231.7.7.7:9876");

        this.locator1 = ActiveMQClient.createServerLocator("tcp://"+ broker_ip + ":61616");
        this.locator2 = ActiveMQClient.createServerLocator("tcp://"+ broker_ip + ":61616");

        this.locator1.setConfirmationWindowSize(1000000).setBlockOnAcknowledge(false).setBlockOnDurableSend(false).setBlockOnNonDurableSend(false);
        this.locator2.setConfirmationWindowSize(1000000).setBlockOnAcknowledge(false).setBlockOnDurableSend(false).setBlockOnNonDurableSend(false);

        this.clientSessionFactory1 = this.locator1.createSessionFactory();
        this.clientSessionFactory2 = this.locator2.createSessionFactory();




        this.clientSession1 = this.clientSessionFactory1.createTransactedSession();


        this.clientSession1.setSendAcknowledgementHandler(new MySendAcknowledgementsHandler());



        this.clientSession2 = this.clientSessionFactory2.createTransactedSession();
        this.clientSession2.setSendAcknowledgementHandler(new MySendAcknowledgementsHandler());



        System.out.println("Connected to Artemis Broker");


        //String FQQN = "experiments::" + "input-queue";
        //String FQQN2 = "experiments::" + "input-queue-2";

        this.producer1 = this.clientSession1.createProducer(topologyAddress);
        this.producer2 = this.clientSession2.createProducer(topologyAddress);









        this.payload = new StringBuilder();

        this.tuple_payload_size_bytes = this.tuple_payload_size_bytes /2; // each char is 2 bytes so we only need half the amount of characters

        for(int i = 0;i < this.tuple_payload_size_bytes; i++){
            this.payload.append("A");
        }


        //System.out.println("Max Producer amount: " + this.locator.getProducerMaxRate());
        System.out.println("Tuple Payload size in bytes : " + this.payload.toString().length()*2);



        int i = 0;
        while (i< superTupleAmount) {
            Tuple tupleToSend = new Tuple();
            //tupleToSend.setPayload("Test Source TP_001: A tuple from Test Source");
            tupleToSend.setPayload(this.payload.toString());
            tupleToSend.setType("A");
            tupleBatch.add(tupleToSend);
            i++;
        }


    }


    void sendTuple(ClientProducer producer, ClientSession session, int superTupleAmount) throws ActiveMQException, IOException {

      /*  //convert Tuple to byte array
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutput out = null;
        byte[] yourBytes;
        try {
            out = new ObjectOutputStream(bos);
            out.writeObject(tupleBatch);
            out.flush();
            yourBytes = bos.toByteArray();

        } finally {
            try {
                bos.close();
                out.close();
            } catch (IOException ex) {
                // ignore close exception
            }
        }

        //attached the byte array to an input stream
        ByteArrayInputStream bis = new ByteArrayInputStream(yourBytes);*/


        //1. create the message object
        ClientMessage msg_to_send = session.createMessage(true);
       /* msg_to_send.setBodyInputStream(bis);
        bis.close();*/


        SimpleTuple object = new SimpleTuple("the_payload");
        System.out.println("Before: " + object.getPayload());
        //object.value = "Hello Kryo!";
        Output output = new Output(1024, -1);

        kryo.writeObject(output, object);
        output.close();

        byte[] bytesToSend = output.getBuffer();
        msg_to_send.putBytesProperty("payload",bytesToSend);
        msg_to_send.putIntProperty("position", output.position());
        msg_to_send.putIntProperty("offset", 0);
        msg_to_send.putStringProperty("name","brian");

        //2. set the timestamp
        msg_to_send.setTimestamp(System.currentTimeMillis());
        //3. set the tuple type (as an attribute)


        //msg_to_send.putStringProperty("tupleType", tupleToSend.getType());

        //5. send the message to the queue(this will read the byte stream and put it into the ActiveMQ msg)
        producer.send(msg_to_send);
        //System.out.println("Tuple sent");


    }


    @Override
    public void run() {

        PrintWriter writer = null;
        //String filename =  "/home/ubuntu/logs/artemis.spout."+ this.topologyAddress + ".exp.log." + currentThread().getName();
        String filename =  "C:\\bench\\artemis.spout."+ this.topologyAddress + ".exp.log." + currentThread().getName();
        try {
            writer = new PrintWriter(new FileOutputStream(new File(filename), false));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        /** Statistics* */
        long currentRecordcount = 0l;
        long windowStartTime  = System.nanoTime();
        long runningTotal =0l;
        long experimentStartTime = System.nanoTime();


        /** Batch size configuration*/

        long desiredRuntime = 60000000000l * this.desired_runtime; // will run for ten minutes
        boolean isActive = true;



        int sequenceCounter = 0;
        System.out.println("Sending test tuples....");
        while(isActive){
        //for(int i=0; i<cycleCount;i++ ){


            try {
                int x ;
                for (x=0;x<= batchCommitSize;x++){



                    //ClientMessage test_operator_msg1 = this.clientSession1.createMessage(true);
                    //test_operator_msg1.getBodyBuffer().writeString(this.payload.toString());
                    //test_operator_msg1.setTimestamp(System.currentTimeMillis());
                    //this.producer1.send(test_operator_msg1);
                    sendTuple(this.producer1, this.clientSession1, this.superTupleAmount);



                    //ClientMessage test_operator_msg2 = this.clientSession2.createMessage(true);
                    //test_operator_msg2.getBodyBuffer().writeString(this.payload.toString());
                    //test_operator_msg2.setTimestamp(System.currentTimeMillis());
                    //this.producer2.send(test_operator_msg2);
                    //sendTuple(this.producer2, this.clientSession2);

                }


                this.clientSession1.commit();
                //this.clientSession2.commit();


               //Thread.sleep(2000);


                /*****************Log the throughput*******************************/
                long windowEndTime = System.nanoTime();
                long windowDuration = windowEndTime - windowStartTime;

                if (windowDuration > 1000000000l) {

                    writer.println(" ProducerID, SeqNumber, TupleSize,  WindowDuration, Throughput, RunningTotal: " + this.ProducerID + " " + sequenceCounter + " " + superTupleAmount + " " + windowDuration +" " +  currentRecordcount +" " +  runningTotal);
                    System.out.println("ProducerID :" + this.ProducerID + " sent this many tuples in one second: "+ currentRecordcount+ " The running total is: " + runningTotal + " Duration: " + windowDuration);
                    //reset the counter
                    currentRecordcount = 0 ;
                    windowStartTime = windowEndTime ;
                    sequenceCounter++;
                }else{

                    currentRecordcount = currentRecordcount + batchCommitSize;
                }



                runningTotal = runningTotal + batchCommitSize;
                /*********************************************************************/

                if(System.nanoTime() - experimentStartTime > desiredRuntime) {
                    isActive = false;
                    System.out.println("Runtime has expired, shutting down experiment");
                }

            } catch (ActiveMQException | IOException e) {
                e.printStackTrace();
            }

        }


        try{

            this.clientSession1.close();
            //this.clientSession2.close();


            //writer.flush();
            writer.close();

            long experimentEndTime = System.nanoTime();

            long experimenRunTime = experimentEndTime - experimentStartTime;

            System.out.println("Experiment Run time: " + experimenRunTime / 1000000000l);
            System.out.println("Throughput tuples/sec: " + ((runningTotal) / (experimenRunTime / 1000000000l)) * superTupleAmount);
            System.out.println("Tuples per SuperTuple: " + superTupleAmount);
            System.out.println("Tuple Size in (Bytes): " + this.payload.toString().length()*2 );

            //System.exit(0);
        } catch (ActiveMQException e) {
            e.printStackTrace();
        }




    }


    class MySendAcknowledgementsHandler implements SendAcknowledgementHandler {

        int count = 0;

        @Override
        public void sendAcknowledged(final Message message) {


            //System.out.println("Received send acknowledgement for message " + count++);

        }
    }

}


