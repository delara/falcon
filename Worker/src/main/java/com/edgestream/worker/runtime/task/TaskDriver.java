package com.edgestream.worker.runtime.task;


import com.edgestream.worker.runtime.task.broker.TaskBrokerConfig;
import com.edgestream.worker.runtime.docker.DockerHostManager;
import com.edgestream.worker.runtime.topology.Topology;
import java.util.ArrayList;

public class TaskDriver {


    private final ArrayList<TaskRunner> taskRunnerList ;
    private final ArrayList<Topology> runningTopologies;
    private final TaskBrokerConfig taskBrokerConfig;
    private final DockerHostManager dockerHostManager;
    private final String defaultMetricsAddress;
    private final int numberOfBridges;


    /**
     * The TaskDriver manages the Runners and Topologies TODO: maybe the topologies should manage the runners, since one topology can have many operators(runners)
     * @param taskRunnerList
     * @param runningTopologies
     * @param taskBrokerConfig
     * @param numberOfBridges
     */
    public TaskDriver(ArrayList<TaskRunner> taskRunnerList, ArrayList<Topology> runningTopologies, TaskBrokerConfig taskBrokerConfig, DockerHostManager dockerHostManager, String defaultMetricsAddress, int numberOfBridges) {

        this.taskRunnerList = taskRunnerList;
        this.runningTopologies = runningTopologies;
        this.taskBrokerConfig = taskBrokerConfig;
        this.dockerHostManager = dockerHostManager;
        this.defaultMetricsAddress = defaultMetricsAddress;
        this.numberOfBridges = numberOfBridges;

    }


    public int getNumberOfBridges() {
        return numberOfBridges;
    }

    public DockerHostManager getDockerHostManager() {
        return dockerHostManager;
    }

    public String getDefaultMetricsAddress() {
        return defaultMetricsAddress;
    }


    public TaskBrokerConfig getTaskBrokerConfig() {
        return taskBrokerConfig;
    }



    public void addTaskRunner(TaskRunner taskRunner){

        this.taskRunnerList.add(taskRunner);

    }

    public void addTopology(Topology topology){
        this.runningTopologies.add(topology);
    }

    public boolean topologyAlreadyExists(String newTopologyID) {

        System.out.println("Checking to see if topology already exists");
        boolean exists = false;

        for (Topology topology :runningTopologies){

            if(topology.getTopologyID().equals(newTopologyID)){
                System.out.println("Topology already exists, do not create a new one");
                exists = true;
            }

        }
        //if no topology was found this should still be false
        if (exists == false){
            System.out.println("Topology does not exist, go ahead and create");

        }
        return exists;
    }

    public Topology getTopology(String topologyIDToFind) {

        Topology foundTopology = null;

        for (Topology topology : runningTopologies) {

            if (topology.getTopologyID().equals(topologyIDToFind)) {
                foundTopology = topology;
                System.out.println("Found existing topology");

            }else {

                System.out.println("Could not find Topology");
            }
        }

        return foundTopology;

    }

    public void printActiveTopologyList() {

        System.out.println("----------Active Topologies -----------");
        for(Topology topology: this.runningTopologies) {

            System.out.println("Topology ID: "+topology.getTopologyID());
            System.out.println("Topology Primary Address: "+topology.getPrimaryTopologyAddress());

        }
    }



}
