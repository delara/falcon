package com.edgestream.client.warmer;

import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;


public class ProducerPubReceiver implements Runnable{

        private final ZMQ.Socket subSocket;
        private Thread t;
        private final String tName;
        private int warmUpTupleReceivedCount = 0;

        public ProducerPubReceiver(ZContext context, String tName) {
            this.tName = tName;
            subSocket = context.createSocket(SocketType.SUB);
            subSocket.subscribe("".getBytes());
            subSocket.setReceiveTimeOut(5000);
            subSocket.connect("tcp://*:9001");
        }

        @Override
        public void run() {
            while(!Thread.interrupted()) {
                byte[] incomingPayloadByteArray = subSocket.recv();

                if(incomingPayloadByteArray != null) {
                    //System.out.println("[ProducerPubReceiver]: I received a warm up tuple from my producer!");
                    //System.out.println(incomingPayloadByteArray);

                    warmUpTupleReceivedCount++;
                }else{
                    System.out.println("[ProducerPubReceiver]: No warm up msg received in the last 5 seconds");
                    System.out.println("[ProducerPubReceiver]: Tuples received since the start: [" + warmUpTupleReceivedCount + "]");
                }
            }
        }

        public void start() {
            System.out.println("[ProducerPubReceiver]: Starting " +  this.tName );
            if (t == null) {
                t = new Thread (this, tName);
                t.start();
            }
        }

        public void interrupt() {
            t.interrupt();
        }
}

