package com.edgestream.worker.metrics.metricscollector2.io;

import com.edgestream.worker.metrics.common.OutputEventMetric;
import org.apache.commons.lang3.SerializationUtils;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

public class ContainerOutputEventWriter {

    private ZContext context;
    private ZMQ.Socket sender;

    public ContainerOutputEventWriter(String metricsOutputEventPortNumber){

        System.out.println("[ContainerOutputEventWriter] Creating new writer to send OUTPUT events..........");

        try {
            this.context = new ZContext();
            //  Socket to send metrics messages on
            this.sender = context.createSocket(SocketType.PUSH);
            this.sender.connect("tcp://*:" + metricsOutputEventPortNumber);

        }catch(Exception exception){
            exception.printStackTrace();
        }


    }

    public void sendOutputEvent(OutputEventMetric outputEventMetric) {
        byte[] dataToSend = SerializationUtils.serialize(outputEventMetric);

        try {
            //System.out.println("Sending tuples to metrics OUTPUT port\n");
            //  The first message is "0" and signals start of batch
            //sender.send("0", 0);
            sender.send(dataToSend, 0);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }


}
