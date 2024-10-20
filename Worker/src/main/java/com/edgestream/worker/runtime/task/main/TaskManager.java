package com.edgestream.worker.runtime.task.main;


import com.edgestream.worker.config.EdgeStreamGetPropertyValues;
import com.edgestream.worker.metrics.collector.HardwareStatsCollector;
import com.edgestream.worker.metrics.collector.NetworkStatsCollector;
import com.edgestream.worker.metrics.collector.TrafficCollector;
import com.edgestream.worker.runtime.task.*;
import com.edgestream.worker.runtime.docker.DockerHost;
import com.edgestream.worker.runtime.docker.DockerHostID;
import com.edgestream.worker.runtime.docker.DockerHostManager;
import com.edgestream.worker.runtime.task.broker.TaskBrokerConfig;
import com.edgestream.worker.runtime.topology.Topology;
import java.util.ArrayList;


public class TaskManager {

    private final String taskManagerID;
    private final TaskListener remoteQueueListener;
    //TaskListenerOffline remoteQueueListenerOffline;  //for debugging

    private final ArrayList<TaskRunner> taskRunnerList = new ArrayList<>();
    private final String remoteTaskQueueFQQN;
    private final String remoteClusterManagerIP;
    private final String broker_ip;
    private final TaskDriver taskDriver; //This driver is a reference to all running topologies after the TaskManager constructor has completed
    private final ArrayList<Topology> runningTopologies = new ArrayList<>();
    private final TaskBrokerConfig taskBrokerConfig;
    private final DockerHostManager dockerHostManager;



    private final String parentTaskManagerIP;
    private final String parentTaskManagerID;

    /**Metrics**/
    private final String metricsDefaultAddress = "primary_merlin_metrics_address";
    private final String metricsDefaultQueue = "primary_metrics_queue";
    private final String metricsFQQN = metricsDefaultAddress +"::"+ metricsDefaultQueue;



    public static void main (String[] args) throws Exception {


        String taskManagerID = args[0];
        String local_broker_ip = args[1];
        String parentTaskManagerID = args[2];
        String remoteClusterManager_ip = args[3];
        String platform = args[4];
        String parentTaskManagerIP = args[5];
        String dockerHostIP = args[6];
        int numberOfBridges = Integer.parseInt(args[7]);
        //Double costPerGBSent = Double.parseDouble(args[7]);
        Double costPerGBSent = 0.02;


        System.out.println(" --------------------" + "Starting Task Manager:" + taskManagerID +" -------------------" );
        new TaskManager(taskManagerID,local_broker_ip,parentTaskManagerID,remoteClusterManager_ip, platform, parentTaskManagerIP,dockerHostIP,costPerGBSent,numberOfBridges);

    }



    public TaskManager(String taskManagerID, String local_broker_ip, String parentTaskManagerID, String remoteClusterManager_ip, String platform, String parentTaskManagerIP, String dockerHostIP, Double costPerGBSent, int numberOfBridges) throws Exception {

        this.broker_ip = local_broker_ip; // the broker that this task manager is responsible for, could also be set to localhost if its running locally
        this.parentTaskManagerIP = parentTaskManagerIP;


        /*** Docker config begin **/
        //1. Create the host manager. This can manage many physical docker hosts
        this.dockerHostManager = new DockerHostManager(taskManagerID+ "_DockerHost_Manager_1", metricsDefaultAddress, taskManagerID, this.broker_ip);

        //2. Create docker host. A docker host is one physical machine running the docker service



        /*** Docker config end **/


        /**System Metrics Threads*/
        //String hostDockerHostName = dockerHostID.getDockerHostIDAsString(); //TODO: setup the framework to use external docker hosts since the Worker will run on a different machine
        //HardwareStatsCollector hardwareStatsCollector = new HardwareStatsCollector(platform,metricsDefaultAddress,taskManagerID,this.broker_ip,hostDockerHostName);
        //hardwareStatsCollector.start();

        NetworkStatsCollector networkStatsCollector = new NetworkStatsCollector(taskManagerID,parentTaskManagerID,parentTaskManagerIP,this.broker_ip,metricsDefaultAddress);
        networkStatsCollector.start();

        TrafficCollector trafficCollector = new TrafficCollector(taskManagerID,parentTaskManagerID,this.broker_ip,parentTaskManagerIP,metricsDefaultAddress,broker_ip,costPerGBSent);
        trafficCollector.start();





        /*** Task Manager setup begin **/
        this.taskManagerID = taskManagerID; // This is how the cluster manager identifies this worker node
        this.remoteClusterManagerIP = remoteClusterManager_ip; // the cluster manager
        this.remoteTaskQueueFQQN = "pending_tasks::" + taskManagerID; //where we listen for new incoming task requests from the cluster manager

        //this.parentIP = parentIP; //the parent that this broker will be bridged to. Bridges are created when topologies are instantiated
        this.parentTaskManagerID = parentTaskManagerID; // currently not used
        this.taskBrokerConfig = new TaskBrokerConfig(this.broker_ip, this.taskManagerID);
        this.taskDriver = new TaskDriver(taskRunnerList,runningTopologies,taskBrokerConfig,dockerHostManager,metricsDefaultAddress,numberOfBridges);
        /*** Task Manager setup end **/



        /*** Task Listener setup begin **/
        System.out.println(" --------------------" + "Launching the Task Listener");
        this.remoteQueueListener = new TaskListener(this.remoteTaskQueueFQQN, this.remoteClusterManagerIP, this.taskDriver);  //Launch the task listener
        this.remoteQueueListener.start(); //Start the task listener
        /*** Task Lister setup end **/


        /**FOR DEBUG***/
        //this.remoteQueueListenerOffline = new TaskListenerOffline(this.taskDriver); //Launch the task listener DEBUGGING
        //this.remoteQueueListenerOffline.run();

    }


}
