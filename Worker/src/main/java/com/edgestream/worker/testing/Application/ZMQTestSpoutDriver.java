package com.edgestream.worker.testing.Application;

import com.edgestream.worker.common.Tuple;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import org.apache.commons.lang3.SerializationUtils;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;


import java.io.*;
import java.util.ArrayList;

public class ZMQTestSpoutDriver {

    public static void main(final String[] args) throws Exception {


        int number_of_producers =  Integer.parseInt(args[0]);
        int tuple_payload_size  = Integer.parseInt(args[1]);
        int batchCommitSize  = Integer.parseInt(args[2]);


        for (int i=0; i < number_of_producers;i++){

            new ZMQSpout("Producer : " + i , i, tuple_payload_size).run();
        }


    }
}


class ZMQSpout extends Thread {

    Integer ProducerID;
    int tuple_payload_size_bytes;
    StringBuilder payload;

    long desired_runtime = 10;
    String topologyAddress;
    int superTupleAmount;

    ArrayList<Tuple> tupleBatch = new ArrayList<>();
    Kryo kryo;
    private final ZContext context;
    private final ZMQ.Socket sender;
    private final String outputSocketPort;


    public ZMQSpout(String name, Integer ProducerID, int tuple_payload_size) {

        this.ProducerID = ProducerID;
        this.tuple_payload_size_bytes = tuple_payload_size;

        this.outputSocketPort = "8000"; // should be 8000

        kryo = new Kryo();
        kryo.register(SimpleTuple.class);

        System.out.println("Starting producer: " + name);



        //ZMQ setup
        this.context = new ZContext();
        //  Socket to send messages on
        this.sender = context.createSocket(SocketType.PUSH);

        //this.sender.setConflate(true);
        //this.sender.setHWM(12);
        this.sender.connect("tcp://broker4cpu-second:" + this.outputSocketPort);

    }


    boolean sendTuple() {

        boolean sent =false;
        SimpleTuple simpleTuple = new SimpleTuple("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA");
        //System.out.println("Before: " + object.getPayload());
        Output output = new Output(1024, -1);
        kryo.writeObject(output, simpleTuple);
        output.close();
        byte[] bytesToSend = output.getBuffer();

        //byte[] bytesToSend = SerializationUtils.serialize(simpleTuple);
        try {
             sent = sender.send(bytesToSend, 0);
        } catch (Exception e) {
            e.printStackTrace();
        }

     return sent;
    }


    @Override
    public void run() {

        PrintWriter writer = null;
        String filename =  "/home/ubuntu/logs/ZMQ.spout.exp.log." + currentThread().getName();
        //String filename =  "C:\\bench\\artemis.spout."+ this.topologyAddress + ".exp.log." + currentThread().getName();
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

        while(isActive) {

            boolean sent = sendTuple();
            if (sent){
                /*****************Log the throughput*******************************/
                long windowEndTime = System.nanoTime();
                long windowDuration = windowEndTime - windowStartTime;

                if (windowDuration > 1000000000l) {

                    writer.println(" ProducerID, SeqNumber, TupleSize,  WindowDuration, Throughput, RunningTotal: " + this.ProducerID + " " + sequenceCounter + " " + superTupleAmount + " " + windowDuration + " " + currentRecordcount + " " + runningTotal);
                    System.out.println("ProducerID :" + this.ProducerID + " sent this many tuples in one second: " + currentRecordcount + " The running total is: " + runningTotal + " Duration: " + windowDuration);
                    //reset the counter
                    currentRecordcount = 0;
                    windowStartTime = windowEndTime;
                    sequenceCounter++;
                } else {

                    currentRecordcount = currentRecordcount + 1;
                }


                runningTotal = runningTotal + 1;
                /*********************************************************************/

                if (System.nanoTime() - experimentStartTime > desiredRuntime) {
                    isActive = false;
                    System.out.println("Runtime has expired, shutting down experiment");
                }
            }


        }//end of while loop

        writer.close();

        long experimentEndTime = System.nanoTime();

        long experimenRunTime = experimentEndTime - experimentStartTime;

        System.out.println("Experiment Run time: " + experimenRunTime / 1000000000l);
        System.out.println("Throughput tuples/sec: " + ((runningTotal) / (experimenRunTime / 1000000000l)) * superTupleAmount);
        System.out.println("Tuples per SuperTuple: " + superTupleAmount);
        System.out.println("Tuple Size in (Bytes): " + this.payload.toString().length() * 2);

    }//end of run method



}//end of class


