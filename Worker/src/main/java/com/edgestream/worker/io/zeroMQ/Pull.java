package com.edgestream.worker.io.zeroMQ;

import com.edgestream.worker.common.Tuple;
import com.edgestream.worker.common.TupleHeader;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

public class Pull {


    public static void main(String[] args) {

        System.out.println("Waiting for a tweet....");
        ZContext context;
        ZMQ.Socket receiver;
        String serviceIP = "*";
        String servicePort = "7002";

        context = new ZContext();
        Kryo kryo = new Kryo();
        kryo.register(TupleHeader.class);
        kryo.register(byte[].class);
        kryo.register(Tuple.class);

        receiver = context.createSocket(SocketType.PULL);
        receiver.bind("tcp://" + serviceIP + ":" + servicePort);
        //receiver.connect("tcp://" + serviceIP + ":" + "7003");
        int counter = 0;

        while (!Thread.currentThread().isInterrupted()) {
            //counter++;
            //byte[] incomingPayloadByteArray = receiver.recv(0);
            //System.out.println("Received: [" + new String(incomingPayloadByteArray, ZMQ.CHARSET) + "]");
            //System.out.println("Received a tweet [" + counter +"]");

            byte[] incomingPayloadByteArray = receiver.recv();
            if (incomingPayloadByteArray != null) {

                //System.out.println("[ZeroMQConsumerClient]: I received a tuple, sending a copy to the push socket");


                //System.out.println("[ZeroMQConsumerClient]: I received a tuple, sending it to the operator");
                //Tuple tuple = SerializationUtils.deserialize(incomingPayloadByteArray); //KRYO CHANGE

                Input input = new Input(incomingPayloadByteArray); //KRYO CHANGE
                Tuple tuple = kryo.readObject(input, Tuple.class); //KRYO CHANGE


                String transfer = tuple.getTupleHeader().getTimestampTransfer();
                String previous_operator = tuple.getTupleHeader().getPreviousOperator();
                String timeStampFromSource = tuple.getTupleHeader().getTimeStampFromSource();
                String tupleID = tuple.getTupleHeader().getTupleID();
                String key = tuple.getKeyValue();
                String tupleOrigin = tuple.getTupleHeader().getTupleOrigin();

                counter++;
                System.out.println("I received a tweet: " + tuple.getType());
            }else{
                System.out.println("payload was null");
            }
        }




    }
}
