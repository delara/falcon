package com.edgestream.worker.metrics.collector;

import com.edgestream.worker.metrics.model.MetricType;
import com.edgestream.worker.metrics.model.TrafficMetric;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.client.*;

import java.io.BufferedReader;
import java.io.InputStreamReader;


public class TrafficCollector extends Thread{

    private final String metricsAddress;
    private ClientProducer producer;
    private ClientSession session;
    private final String metricsBrokerIP;

    private final String localIP;
    private final String parentIP;
    private String sourceStringBuffer = "";
    private String destinationStringBuffer = "";
    private final String localTaskManagerID;
    private final String parentTaskManagerID;
    private boolean updateInProcess = false;
    private Double costPerGBSent = 0.00;


    /**FOR DEBUG***/
    public static void main(String[] args) {
        new TrafficCollector( "W002","W001","10.70.2.106",  "10.70.2.224","","", 0.00).start();
    }


    public TrafficCollector(String localTaskManagerID, String parentTaskManagerID, String localIP, String parentIP, String metricsAddress, String metricsBrokerIP, Double costPerGBSent){

        this.localIP = localIP;
        this.parentIP = parentIP;
        this.metricsAddress = metricsAddress;
        this.metricsBrokerIP = metricsBrokerIP;
        this.localTaskManagerID = localTaskManagerID;
        this.parentTaskManagerID = parentTaskManagerID;
        this.costPerGBSent = costPerGBSent;

    }

    public void run() {
        if (!localIP.equals(parentIP)) { // These values should be different because the child and parent must have different IPs

            System.out.println("[TrafficCollector]:  Starting Traffic collector.......");

            createAndSetActiveMQClient();

            try {
                ProcessBuilder processBuilder = new ProcessBuilder("sudo", "/bin/sh", "-c", "iftop -t -n");
                Process proc = processBuilder.start();
                BufferedReader stdInput = new BufferedReader(new InputStreamReader(proc.getInputStream()));
                BufferedReader stdError = new BufferedReader(new InputStreamReader(proc.getErrorStream()));

                // Read the output from the command
                //System.out.println("Here is the standard output of the command:\n");

                String s = null;
                while ((s = stdInput.readLine()) != null) { // This reads forever
                    updateTrafficStats(s);
                    //System.out.println("---------------");
                }

                // Read any errors from the attempted command
                //System.out.println("Here is the standard error of the command (if any):\n");
                while ((s = stdError.readLine()) != null) {
                    System.out.println(s);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            System.out.println("[TrafficCollector]:  Traffic collector exited for some reason, this should not happen, investigate!");
        }

    }

    private void updateTrafficStats(String ifTopOutputLine){

        if (!this.updateInProcess) {
            if (ifTopOutputLine.contains("=>") && ifTopOutputLine.contains(this.localIP)) {
                sourceStringBuffer = ifTopOutputLine;
                this.updateInProcess = true;
                //System.out.println("Found local ip line");
            }
        }else{
            if (ifTopOutputLine.contains(this.parentIP)) {
                destinationStringBuffer = ifTopOutputLine;
                processTrafficMetric(sourceStringBuffer); //extracts the bytes sent and then creates and sends the metric value
                //printUpdate();
            }else{
                //System.out.println("This line is no good, I am skipping it....[" + ifTopOutputLine + "]" );
            }
            this.updateInProcess = false;
        }
    }

    private void printUpdate(){

        System.out.println("-------------------------------------------------------------->[TrafficCollector]: sourceStringBuffer ->[" + sourceStringBuffer +"]");
        System.out.println("-------------------------------------------------------------->[TrafficCollector]: destinationStringBuffer ->[" + destinationStringBuffer+ "]");
    }

    private void processTrafficMetric(String sourceStringBuffer){

        long bytesSent = extractTrafficValue(sourceStringBuffer);
        TrafficMetric trafficMetric = new TrafficMetric(String.valueOf(System.currentTimeMillis()),localTaskManagerID,localIP,parentTaskManagerID,parentIP,bytesSent);
        writeMetricsMessageToBroker(trafficMetric);

    }



    private long extractTrafficValue(String sourceStringBuffer){

        String[] splited = sourceStringBuffer.split("\\s+");
        int len = splited.length;
        String displayedTrafficValue = splited[len-1];
        //System.out.println("Total displayed sent ["+displayedTrafficValue+"]");

        return convertToBytes(displayedTrafficValue); // will return 0 or the real value
    }

    private long convertToBytes(String displayedTrafficValue){

        long totalBytesSent = 0l;
        String extractedValue = "";

        //we only start reporting when the data is > than MB
        if(displayedTrafficValue.contains("MB")){
            String[] val = displayedTrafficValue.split("MB");
            extractedValue = val[0];
            Float floatBytesSent = Float.parseFloat(extractedValue) *  1048576l;
            totalBytesSent = floatBytesSent.longValue();
        }

        if(displayedTrafficValue.contains("GB")){
            String[] val = displayedTrafficValue.split("GB");
            extractedValue = val[0];
            Float floatBytesSent = Float.parseFloat(extractedValue) *  1073741824l;
            totalBytesSent = floatBytesSent.longValue();
        }

        if(displayedTrafficValue.contains("TB")){
            String[] val = displayedTrafficValue.split("TB");
            extractedValue = val[0];
            Float floatBytesSent = Float.parseFloat(extractedValue) *  1099511627776l;
            totalBytesSent = floatBytesSent.longValue();
        }

        //System.out.println("Total bytes sent: [" + totalBytesSent + "]");
        return totalBytesSent;
    }




    private void writeMetricsMessageToBroker(TrafficMetric trafficMetric)  {

        //1. create the message object
        ClientMessage msg_to_send = session.createMessage(true);

        //2. set the timestamp
        msg_to_send.setTimestamp(System.currentTimeMillis());

        //3. set the metrics data on the message
        msg_to_send.putStringProperty("message_type", MetricType.TRAFFIC.toString());
        msg_to_send.putStringProperty("timeStamp", trafficMetric.getTimeStamp());
        msg_to_send.putStringProperty("taskManagerID", trafficMetric.getLocalTaskManagerID());
        msg_to_send.putStringProperty("parentTaskManagerID", trafficMetric.getParentTaskManagerID());


        msg_to_send.putStringProperty("source", trafficMetric.getLocalTaskManagerIP());
        msg_to_send.putStringProperty("destination", trafficMetric.getParentTaskManagerIP());

        msg_to_send.putStringProperty("bytesSent", String.valueOf(trafficMetric.getBytesSent()));


        //4. send to the broker and commit
        try {
            producer.send(msg_to_send);
            session.commit();
        }catch(ActiveMQException activeMQException){
            activeMQException.printStackTrace();
        }

    }


    private void createAndSetActiveMQClient(){
        System.out.println("Starting ActiveMQ [TRAFFIC] metrics writer: ");
        try{
            ServerLocator locator;
            locator = ActiveMQClient.createServerLocator("tcp://" + metricsBrokerIP + ":61616");

            ClientSessionFactory factory = locator.createSessionFactory();
            session = factory.createSession();
            session.start();
            producer = session.createProducer(this.metricsAddress);
        }catch(Exception e){
            e.printStackTrace();
        }
    }


}
