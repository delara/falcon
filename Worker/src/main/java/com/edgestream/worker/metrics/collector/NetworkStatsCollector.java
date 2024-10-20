package com.edgestream.worker.metrics.collector;

import com.edgestream.worker.metrics.model.MetricType;
import com.edgestream.worker.metrics.model.NetworkSystemMetric;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.client.*;

public class NetworkStatsCollector extends Thread {

    private final String metricsAddress;
    private ClientProducer producer;
    private ClientSession session;
    private final String taskManagerID;
    private final String parentTaskManagerID;
    private final String parentTaskManagerIP;
    private final String brokerIP;

    public NetworkStatsCollector(String taskManagerID, String parentTaskManagerID, String parentTaskManagerIP,  String brokerIP, String metricsAddress ){

        this.taskManagerID = taskManagerID;
        this.parentTaskManagerID = parentTaskManagerID;
        this.parentTaskManagerIP = parentTaskManagerIP;
        this.brokerIP = brokerIP;
        this.metricsAddress = metricsAddress;

        this.createAndSetActiveMQClient();

    }

    public void run() {

        while (true) {


            //System.out.println("--------Network Stats-----------------------");

            String timeStamp = String.valueOf(System.currentTimeMillis());
            String taskManagerID = this.taskManagerID;
            String parentTaskManagerID = this.parentTaskManagerID;
            String parentTaskManagerIP = this.parentTaskManagerIP;


            NetworkSystemMetric networkSystemMetric  = new NetworkSystemMetric(timeStamp,taskManagerID,parentTaskManagerID,parentTaskManagerIP);

            try {
                writeMetricsMessageToBroker(networkSystemMetric);
            } catch (ActiveMQException e) {
                e.printStackTrace();
            }


            try {
                Thread.sleep(1000 * 5);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        }

    }





    private void writeMetricsMessageToBroker(NetworkSystemMetric networkSystemMetric) throws ActiveMQException {

        //1. create the message object
        ClientMessage msg_to_send = session.createMessage(true);

        //2. set the timestamp
        msg_to_send.setTimestamp(System.currentTimeMillis());

        //3. set the metrics data on the message
        msg_to_send.putStringProperty("message_type", MetricType.NETWORK.toString());
        msg_to_send.putStringProperty("timeStamp", networkSystemMetric.getTimeStamp());
        msg_to_send.putStringProperty("taskManagerID", networkSystemMetric.getTaskManagerID());
        msg_to_send.putStringProperty("parentTaskManagerID", networkSystemMetric.getParentTaskManagerID());
        msg_to_send.putStringProperty("parentTaskManagerIP", networkSystemMetric.getParentTaskManagerIP());
        msg_to_send.putStringProperty("linkID", networkSystemMetric.getLinkID());
        msg_to_send.putStringProperty("available_bandwidth_mbps", networkSystemMetric.getAvailable_bandwidth_mbps());
        msg_to_send.putStringProperty("average_network_latency", networkSystemMetric.getAverage_network_latency());


        //4. send to the broker and commit
        producer.send(msg_to_send);
        session.commit();

    }


    private void createAndSetActiveMQClient(){

        System.out.println("Starting ActiveMQ [NETWORK] metrics writer: ");

        try{

            ServerLocator locator;
            locator = ActiveMQClient.createServerLocator("tcp://" + brokerIP + ":61616");


            ClientSessionFactory factory = locator.createSessionFactory();

            session = factory.createSession();
            session.start();
            producer = session.createProducer(this.metricsAddress);

        }catch(Exception e){
            e.printStackTrace();
        }

    }
}
