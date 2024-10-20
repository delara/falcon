package com.edgestream.worker.metrics.collector;

import com.edgestream.worker.metrics.model.MetricType;
import com.edgestream.worker.metrics.model.DockerSystemMetric;
import com.edgestream.worker.runtime.container.EdgeStreamContainer;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.client.*;
import org.apache.commons.io.IOUtils;
import org.json.JSONObject;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.text.DecimalFormat;
import java.util.concurrent.CopyOnWriteArrayList;

public class DockerStatsCollector extends Thread{


    private final String metricsAddress;
    private final CopyOnWriteArrayList<EdgeStreamContainer> activeEdgeStreamContainers;
    private ClientProducer producer;
    private ClientSession session;
    private final String brokerIP;
    private final String taskManagerID;
    private final String dockerHostIP;

    /**
     * This class will collect the metrics data for every container running on the host that is accessible
     * by the provided {@link com.github.dockerjava.api.DockerClient} in the constructor. The metrics address is set

     * @param metricsAddress
     */
    public DockerStatsCollector(String dockerHostIP, String metricsAddress, String taskManagerID, String brokerIP, CopyOnWriteArrayList<EdgeStreamContainer> activeEdgeStreamContainers) throws IOException {

        this.dockerHostIP = dockerHostIP;
        this.metricsAddress = metricsAddress;
        this.activeEdgeStreamContainers = activeEdgeStreamContainers;
        this.brokerIP = brokerIP;
        this.taskManagerID = taskManagerID;
        this.createAndSetActiveMQClient();



    }

    @Override
    /**
     * This thread will loop indefinitely gathering the metrics data from all containers and writing the output to the message broker
     */
    public void run() {

        while(true){

            for(EdgeStreamContainer edgeStreamContainer : this.activeEdgeStreamContainers) {


                JSONObject stats = null;
                try {
                    stats = getDockerStats(edgeStreamContainer);
                } catch (IOException e) {
                    e.printStackTrace();
                }



                String currentCPUUsage = "0";
                try {
                    currentCPUUsage = calculateCPUUsage(stats);
                } catch (IOException e) {
                    e.printStackTrace();
                }


                    //System.out.println("--------Container Stats-----------------------");
                    //System.out.println("EdgeStream Container ID: " + edgeStreamContainer.getEdgeStreamContainerID().getEdgeStreamContainerIDAsString());
                    //System.out.println("Docker Container ID: " + edgeStreamContainer.getEdgeStreamContainerID().getDockerContainerID());
                    //System.out.println("Container cpu usage: " + currentCPUUsage);
                    //System.out.println("Container memory usage: " + memoryUsageInBytes(stats));


                    String timeStamp = String.valueOf(System.currentTimeMillis());
                    String taskManagerID = this.taskManagerID;
                    String operator_id = edgeStreamContainer.getOperator_id();
                    String container_id = edgeStreamContainer.getEdgeStreamContainerID().getDockerContainerID();
                    String container_name = edgeStreamContainer.getEdgeStreamContainerID().getDockerContainerName();
                    String container_cpu_utilization = currentCPUUsage;
                    String container_allocated_cpu_core_count = getCPUCoreCount(stats);
                    String container_memory_usage = memoryUsageInBytes(stats);
                    String container_memory_limit = memoryLimitInBytes(stats);


                    DockerSystemMetric dockerSystemMetric = new DockerSystemMetric(timeStamp,taskManagerID,operator_id,container_id,container_name,container_cpu_utilization, container_allocated_cpu_core_count, container_memory_usage,container_memory_limit);

                    try {
                        writeMetricsMessageToBroker(dockerSystemMetric);
                    } catch (ActiveMQException e) {
                        e.printStackTrace();
                    }

                }


                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

    }


    private void writeMetricsMessageToBroker(DockerSystemMetric dockerSystemMetric) throws ActiveMQException {

        //1. create the message object
        ClientMessage msg_to_send = session.createMessage(true);

        //2. set the timestamp
        msg_to_send.setTimestamp(System.currentTimeMillis());

        //3. set the metrics data on the message
        msg_to_send.putStringProperty("message_type", MetricType.DOCKER.toString());
        msg_to_send.putStringProperty("timeStamp", dockerSystemMetric.getTimeStamp());
        msg_to_send.putStringProperty("taskManagerID", dockerSystemMetric.getTaskManagerID());
        msg_to_send.putStringProperty("operator_id", dockerSystemMetric.getOperator_ID());
        msg_to_send.putStringProperty("container_id", dockerSystemMetric.getDockerContainer_id());
        msg_to_send.putStringProperty("container_name", dockerSystemMetric.getDockerContainer_name());
        msg_to_send.putStringProperty("container_cpu_utilization", dockerSystemMetric.getCpu_utilization());
        msg_to_send.putStringProperty("container_allocated_cpu_core_count", dockerSystemMetric.getCpu_core_count());
        msg_to_send.putStringProperty("container_memory_usage", dockerSystemMetric.getMemory_usage());
        msg_to_send.putStringProperty("container_memory_limit", dockerSystemMetric.getMemory_limit());

        //4. send to the broker and commit
        producer.send(msg_to_send);
        session.commit();


    }

    private void createAndSetActiveMQClient(){

        System.out.println("Starting ActiveMQ [DOCKER] metrics writer: ");

        try {

            ServerLocator locator;
            //ServerLocator locator = ActiveMQClient.createServerLocator("tcp://localhost:61616");
            locator = ActiveMQClient.createServerLocator("tcp://" + brokerIP + ":61616");
            //locator.setConfirmationWindowSize(300000);

            ClientSessionFactory factory = locator.createSessionFactory();

            session = factory.createSession();
            //session.setSendAcknowledgementHandler(new ArtemisProducer.MySendAcknowledgementsHandler());


            //System.out.println("Connected to Artemis Broker");

            session.start();
            producer = session.createProducer(this.metricsAddress);

        }catch(Exception e){
            e.printStackTrace();
        }

    }


    private JSONObject getDockerStats(EdgeStreamContainer edgeStreamContainer) throws IOException {

        String URL = "http://" + this.dockerHostIP + ":2375/containers/" + edgeStreamContainer.getEdgeStreamContainerID().getDockerContainerID()+"/stats?stream=false";

        JSONObject json = new JSONObject(IOUtils.toString(new URL(URL), StandardCharsets.UTF_8));

        return json;

    }

    private String getCPUCoreCount(JSONObject json){
        int numberOfCpu =0;
        try {
            numberOfCpu = Integer.parseInt(new JSONObject(json.get("cpu_stats").toString()).get("online_cpus").toString());
        }
        catch (Exception e){
            System.out.println("CPU count stats not ready, will report 0");
        }


        return  String.valueOf(numberOfCpu);

    }

    private String memoryUsageInBytes(JSONObject json){
        long memUsage =0l;

        try {
            memUsage = Long.parseLong(new JSONObject(json.get("memory_stats").toString()).get("usage").toString());
        } catch (Exception e){

            System.out.println("Memory usage is not ready, will report 0");

        }

        return  String.valueOf(memUsage);

    }


    private String memoryLimitInBytes(JSONObject json){
        long memLimit =0l;

        try {
            memLimit = Long.parseLong(new JSONObject(json.get("memory_stats").toString()).get("limit").toString());
        }catch (Exception e){

            System.out.println("Memory limit is not ready, will report 0");


        }


        return  String.valueOf(memLimit);

    }

    private String calculateCPUUsage(JSONObject json) throws IOException {

        //System.out.println(json.toString());

        //System.out.println(new JSONObject(new JSONObject(json.get("cpu_stats").toString()).get("cpu_usage").toString()).get("total_usage").toString());
        //System.out.println(new JSONObject(new JSONObject(json.get("precpu_stats").toString()).get("cpu_usage").toString()).get("total_usage").toString());


        //System.out.println(new JSONObject(json.get("cpu_stats").toString()).get("system_cpu_usage").toString());
        //System.out.println(new JSONObject(json.get("precpu_stats").toString()).get("system_cpu_usage").toString());

        double cpuPercent = 0.0;

        try {
            int numberOfCpu = Integer.parseInt(new JSONObject(json.get("cpu_stats").toString()).get("online_cpus").toString());

            /**CPU delta*/
            double totalUsage = Double.parseDouble(new JSONObject(new JSONObject(json.get("cpu_stats").toString()).get("cpu_usage").toString()).get("total_usage").toString());
            double prev_totalUsage = Double.parseDouble(new JSONObject(new JSONObject(json.get("precpu_stats").toString()).get("cpu_usage").toString()).get("total_usage").toString());

            /**System delta*/

            double systemTotalUsage = Double.parseDouble(new JSONObject(json.get("cpu_stats").toString()).get("system_cpu_usage").toString());
            double prev_systemTotalUsage = Double.parseDouble(new JSONObject(json.get("precpu_stats").toString()).get("system_cpu_usage").toString());


            double cpuDelta = totalUsage - prev_totalUsage;
            double sysDelta = systemTotalUsage - prev_systemTotalUsage;

            if (sysDelta > 0.0 && cpuDelta > 0.0) {
                cpuPercent = (cpuDelta / sysDelta) * numberOfCpu * 100;
            }



        } catch (Exception e){

            System.out.println("Docker CPU Stats not ready, will report 0.0");

        }

        DecimalFormat df = new DecimalFormat("000.00");

        return df.format(cpuPercent);

    }






}
