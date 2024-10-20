package com.edgestream.worker.io.zeroMQ;

import com.edgestream.worker.common.Tuple;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

public class Pub {
    public static void main(String[] args) throws Exception {
        ZContext context;

        ZMQ.Socket pub1;
        ZMQ.Socket pub2;
        //ZMQ.Socket sub;

        context = new ZContext();
        pub1 = context.createSocket(SocketType.PUB);
        pub2 = context.createSocket(SocketType.PUB);


        pub1.connect("tcp://*:7000");
        //pub2.connect("tcp://*:7003");


        // Eliminate slow subscriber problem
        while(true) {
            //Thread.sleep(1);

            //Tuple testTuple = new Tuple();
            //testTuple.setKeyValue("key");
            pub1.send("Hello, world! 1");
            //pub2.send("Hello, world! 2");
        }


        //sub.close();
        //pub.close();
        //context.close();
    }
}