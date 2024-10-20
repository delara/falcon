package com.edgestream.worker.io.zeroMQ;

import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

public class Push {


    public static void main(String[] args) {

        new Push().createPushSocket();

    }


    private void createPushSocket() {

        ZContext context = new ZContext();
        ZMQ.Socket sender1 = null;
        ZMQ.Socket sender2 = null;
        try {
            //  Socket to send messages on
            sender1 = context.createSocket(SocketType.PUSH);
            sender1.connect("tcp://*:" + "7000");
            System.out.println("[ActiveMQToZeroMQSwitchableMessageConsumerClient]: ZeroMQ Push Socket Created [1]");

            sender2 = context.createSocket(SocketType.PUSH);
            sender2.bind("tcp://*:" + "7003");
            System.out.println("[ActiveMQToZeroMQSwitchableMessageConsumerClient]: ZeroMQ Push Socket Created [2]");

        } catch (Exception e) {
            e.printStackTrace();
        }

        while(true){

            sender1.send("Hello 1");
            sender2.send("Hello 2");
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }


        }

    }



}
