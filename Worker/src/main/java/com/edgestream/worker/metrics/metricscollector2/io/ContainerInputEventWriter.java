package com.edgestream.worker.metrics.metricscollector2.io;

import com.edgestream.worker.metrics.common.InputEventMetric;
import org.apache.commons.lang3.SerializationUtils;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

public class ContainerInputEventWriter {
    private ZContext context;
    private ZMQ.Socket sender;


    public ContainerInputEventWriter(String metricsInputEventPortNumber) {
        System.out.println("[ContainerInputMetricWriter] Creating new writer to send INPUT events..........");

        try {
            this.context = new ZContext();
            //  Socket to send metrics messages on
            this.sender = context.createSocket(SocketType.PUSH);
            this.sender.connect("tcp://*:" + metricsInputEventPortNumber);

        }catch(Exception exception){
            exception.printStackTrace();
        }

    }

    public void sendInputEvent(InputEventMetric inputEventMetric) {
        byte[] dataToSend = SerializationUtils.serialize(inputEventMetric);

        try {
            //System.out.println("Sending tuples to metrics input port\n");
            //  The first message is "0" and signals start of batch
            //sender.send("0", 0);
            sender.send(dataToSend, 0);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
