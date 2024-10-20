package com.edgestream.worker.metrics.collector;

import com.edgestream.worker.metrics.model.MetricType;
import com.edgestream.worker.metrics.exception.EdgeStreamHardwareStatsException;
import com.edgestream.worker.metrics.model.HardwareSystemMetric;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.client.*;
import java.io.BufferedReader;
import java.io.InputStreamReader;


public class HardwareStatsCollector extends Thread{

    private final String metricsAddress;
    private final String platform;
    private String cpuModel;
    private String cpuCores;
    private String MemTotal;
    private String MemAvailable;
    private String MemFree;
    private final String taskManagerID;
    private ClientProducer producer;
    private ClientSession session;
    private final String brokerIP;
    private final String hostName;


    public HardwareStatsCollector(String platform, String metricsAddress, String taskManagerID, String brokerIP, String hostName) {
        this.platform = platform;
        this.metricsAddress = metricsAddress;
        this.taskManagerID = taskManagerID;
        this.brokerIP = brokerIP;
        this.hostName = hostName;

        this.createAndSetActiveMQClient();
        this.setCPUModelAndCores();

    }


    public void run() {
        int  secondsCounter = 1;
        while (true) {

            this.setMemorySpecs(); //memory stats may change over time so need to recheck every time


            System.out.println(secondsCounter +" --------Host Hardware Stats-----------------------");

            String timeStamp = String.valueOf(System.currentTimeMillis());
            String taskManagerID = this.taskManagerID;
            String host_cpu_model = this.cpuModel;
            String host_cpu_number_of_cores = this.cpuCores;
            String availableMemory = this.MemAvailable;
            String totalMemory = this.MemTotal;
            String freeMemory = this.MemFree;
            String platform = this.platform;

            HardwareSystemMetric hardwareSystemMetric = new HardwareSystemMetric(
                    timeStamp
                    ,taskManagerID
                    ,host_cpu_model
                    ,host_cpu_number_of_cores
                    ,availableMemory
                    ,totalMemory
                    ,freeMemory
                    ,platform
                    ,hostName
            );

            try {
                writeMetricsMessageToBroker(hardwareSystemMetric);
            } catch (ActiveMQException e) {
                e.printStackTrace();
            }


            try {
                Thread.sleep(1000);  //every 1 second
            } catch (InterruptedException e) {
                e.printStackTrace();
            }


            secondsCounter++;
            if (secondsCounter == 60){
                secondsCounter = 1;
            }
        }

    }



    private void writeMetricsMessageToBroker(HardwareSystemMetric hardwareSystemMetric) throws ActiveMQException {

        //1. create the message object
        ClientMessage msg_to_send = session.createMessage(true);

        //2. set the timestamp
        msg_to_send.setTimestamp(System.currentTimeMillis());

        //3. set the metrics data on the message
        msg_to_send.putStringProperty("message_type", MetricType.SYSTEM.toString());
        msg_to_send.putStringProperty("timeStamp", hardwareSystemMetric.getTimeStamp());
        msg_to_send.putStringProperty("taskManagerID", hardwareSystemMetric.getTaskManagerID());
        msg_to_send.putStringProperty("host_cpu_model", hardwareSystemMetric.getCpu_model());
        msg_to_send.putStringProperty("host_cpu_number_of_cores", hardwareSystemMetric.getCpu_number_of_cores());
        msg_to_send.putStringProperty("MemAvailable", hardwareSystemMetric.getMemAvailable());
        msg_to_send.putStringProperty("MemTotal", hardwareSystemMetric.getMemTotal());
        msg_to_send.putStringProperty("MemFree", hardwareSystemMetric.getMemFree());
        msg_to_send.putStringProperty("platform", hardwareSystemMetric.getPlatform());
        msg_to_send.putStringProperty("hostName", hardwareSystemMetric.getHostName());


        //4. send to the broker and commit
        producer.send(msg_to_send);
        session.commit();

    }






    private void createAndSetActiveMQClient(){

        System.out.println("Starting ActiveMQ [SYSTEM] metrics writer: ");

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




    private void setCPUModelAndCores() {


        if (platform.equalsIgnoreCase("windows")){
            Process p;
            try {

                ProcessBuilder pb = new ProcessBuilder("wmic", "cpu", "get", "NAME");

                //pb.redirectOutput(ProcessBuilder.Redirect.INHERIT);
                pb.redirectError(ProcessBuilder.Redirect.INHERIT);
                p = pb.start();


                BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()));
                StringBuilder builder = new StringBuilder();
                String line = null;
                while ((line = reader.readLine()) != null) {
                    if (!line.startsWith("Name") && !line.startsWith(" ")) {
                        builder.append(line);
                    }

                }
                String result = builder.toString();

                System.out.println(result);
                //result = builder.toString().split(":")[1].trim();

                this.cpuModel = result;

            }catch (Exception exception){

                throw new EdgeStreamHardwareStatsException("Could not get CPU model and core count from host", exception);
            }

            p.destroy();

        }



        if (platform.equalsIgnoreCase("linux")) {
            Process p;
            try {
                ProcessBuilder pb = new ProcessBuilder("lscpu");
                //pb.redirectOutput(ProcessBuilder.Redirect.INHERIT);
                pb.redirectError(ProcessBuilder.Redirect.INHERIT);
                p = pb.start();

                BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()));
                StringBuilder builder = new StringBuilder();
                String cpuCores = null;
                String line = null;
                while ( (line = reader.readLine()) != null) {

                    if (line.startsWith("Model name")){
                        //System.out.println(line);
                        builder.append(line);
                    }
                    if (line.startsWith("CPU(s):")){
                        //System.out.println(line);
                        cpuCores = line;
                    }

                }


                this.cpuModel =  builder.toString().split(":")[1].trim();
                this.cpuCores = cpuCores.split(":")[1].trim();


            } catch (Exception exception) {
                throw new EdgeStreamHardwareStatsException("Could not get CPU model and core count from host", exception);
            }

            p.destroy();
        }

    }

    private void setMemorySpecs() {

        //TODO: implement windows version of this function. Memory values will be set to null until then

        if (platform.equalsIgnoreCase("linux")) {
            Process p;
            try {
                ProcessBuilder pb = new ProcessBuilder("cat", "/proc/meminfo");
                //pb.redirectOutput(ProcessBuilder.Redirect.INHERIT);
                pb.redirectError(ProcessBuilder.Redirect.INHERIT);
                p = pb.start();

                BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()));
                String memTotal = null;
                String memAvailable = null;
                String memFree = null;
                String line = null;
                while ( (line = reader.readLine()) != null) {

                    if (line.startsWith("MemTotal")){

                        memTotal = line;
                    }
                    if (line.startsWith("MemAvailable")){

                        memAvailable = line;
                    }
                    if (line.startsWith("MemFree")){

                        memFree = line;
                    }


                }

                this.MemTotal = memTotal.split(":")[1].trim().split(" ")[0];
                this.MemAvailable = memAvailable.split(":")[1].trim().split(" ")[0];
                this.MemFree = memFree.split(":")[1].trim().split(" ")[0];


            } catch (Exception exception) {
                throw new EdgeStreamHardwareStatsException("Could not get Memory model and core count from host", exception);
            }

            p.destroy();
        }
    }

}
