package com.edgestream.worker.io.zeroMQ;

import com.edgestream.worker.common.Tuple;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

public class Sub {

 /*   public static void main(String[] args) {
        ZContext context;
        ZMQ.Socket sub;


        context = new ZContext();
        sub = context.createSocket(SocketType.SUB);
        sub.subscribe("".getBytes());
        sub.connect("tcp://127.0.0.1:12345");
        int counter =0 ;
        while (!Thread.currentThread().isInterrupted()) {
            System.out.println("SUB: " + sub.recvStr() + " " + counter);
            counter++;
        }

        sub.close();

        context.close();
    }*/



    public static void main(String[] args) {

        ZContext context;
        ZMQ.Socket sub = null;

        try {
            context = new ZContext();
            //  Socket to consume messages from the source container
            sub = context.createSocket(SocketType.SUB);
            sub.subscribe("".getBytes());
            sub.setReceiveTimeOut(5000);
            //sub.connect("tcp://" + warmupSourceIP + ":" + warmupSourcePort);
            sub.bind("tcp://" + "*" + ":" + "7000");
            sub.bind("tcp://" + "*" + ":" + "7003");

            // This sender will send the copied tuples to the live operator zmq consumer
        } catch (Exception e) {
            e.printStackTrace();
        }



        int counter =0 ;
        while (true) {
            byte[] reply = sub.recv(0);
            System.out.println("Received: [" + new String(reply, ZMQ.CHARSET) + "]");
            //System.out.println("SUB: " + sub.recv() + " " + counter);
            counter++;
        }


    }


}
