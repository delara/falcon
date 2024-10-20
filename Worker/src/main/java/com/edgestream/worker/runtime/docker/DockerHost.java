package com.edgestream.worker.runtime.docker;

import com.edgestream.worker.metrics.collector.DockerStatsCollector;
import com.edgestream.worker.runtime.container.EdgeStreamContainer;


import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.CreateContainerResponse;
import com.github.dockerjava.api.command.InspectContainerResponse;
import com.github.dockerjava.api.model.Container;
import com.github.dockerjava.api.model.Network;
import com.github.dockerjava.core.DockerClientBuilder;


import java.io.IOException;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;

public class DockerHost {

    private final DockerHostID dockerHostID;
    private final String dockerHostIP;
    private final String dockerRemoteAPIPort;
    private final CopyOnWriteArrayList<EdgeStreamContainer> activeEdgeStreamContainers = new CopyOnWriteArrayList<>();
    private final DockerClient dockerClient;
    private final DockerStatsCollector dockerStatsCollector;
    private String topologyDockerRepositoryAndTag;
    private boolean isDockerTopologyRepositoryAndTagSet = false; //false on start
    private final String scheduledHostID;
    private final String taskManagerID;




    public DockerHost(DockerHostID dockerHostID, String taskManagerID, String brokerIP, String dockerHostIP, String scheduledHostID, String dockerRemoteAPIPort, String metricsAddress) throws IOException {
        this.dockerHostID = dockerHostID;
        this.dockerHostIP = dockerHostIP;
        this.dockerRemoteAPIPort = dockerRemoteAPIPort;
        this.scheduledHostID = scheduledHostID;
        this.taskManagerID = taskManagerID;

        System.out.println("[DockerHost]:  creating docker host with the following "
                + "dockerHostID: [" + dockerHostID.getDockerHostIDAsString() + "] --"
                + "dockerHostIP: [" + dockerHostIP + "] --"
                + "scheduledHostID: [" + scheduledHostID + "] --"
                + "taskManagerID: [" + taskManagerID + "]"

        );

        //1. Setup docker client connection
        this.dockerClient = DockerClientBuilder.getInstance("tcp://" + dockerHostIP +":" + dockerRemoteAPIPort).build();
        //2 Setup stats collector using same docker client
        this.dockerStatsCollector = new DockerStatsCollector(this.getDockerHostIP(), metricsAddress, taskManagerID, brokerIP,activeEdgeStreamContainers);
        //3. start the collector thread
        this.dockerStatsCollector.start();

        createNetwork();



    }



    public DockerClient getDockerClient() {
        return dockerClient;
    }

    public DockerHostID getDockerHostID() {
        return dockerHostID;
    }

    public String getDockerHostIP() {
        return dockerHostIP;
    }


    public String getScheduledHostID() {
        return scheduledHostID;
    }


    public String getTaskManagerID() {
        return taskManagerID;
    }

    private String getDockerRemoteAPIPort() {
        return dockerRemoteAPIPort;
    }

    protected List<EdgeStreamContainer> getActiveEdgeStreamContainers() {
        return activeEdgeStreamContainers;
    }


    protected void addEdgeStreamContainer(EdgeStreamContainer edgeStreamContainerToBeAdded){

        this.activeEdgeStreamContainers.add(edgeStreamContainerToBeAdded);
        printEdgeStreamContainerList();

    }

    protected void removeEdgeStreamContainer(EdgeStreamContainer edgeStreamContainerToBeAdded){

        this.activeEdgeStreamContainers.remove(edgeStreamContainerToBeAdded);
        printEdgeStreamContainerList();

    }


    protected void printEdgeStreamContainerList(){

        System.out.println("[DockerHost]: ["+ dockerHostID.getDockerHostIDAsString() + "]--------------Active EdgeStream containers -------");
        for(EdgeStreamContainer esc : activeEdgeStreamContainers){

            System.out.println(esc.getEdgeStreamContainerID().getEdgeStreamContainerIDAsString() + " : " +esc.getEdgeStreamContainerID().getDockerContainerID() + " : " +esc.getDockerContainerLocalIP());

        }

    }



    protected void ifExistsRemoveContainer(String containerName){

        List<String> containerList = new ArrayList<>();
        containerList.add(containerName);

        List<Container> containers = dockerClient.listContainersCmd()
                .withNameFilter(containerList)
                .withShowSize(true)
                .withShowAll(true)
                //.withStatusFilter(Collections.singleton("created"))
                .exec();

        if (containers.size() > 0) {
            for (Container c : containers) {

                String[] container_name = c.getNames();
                System.out.println("Found matching container: " + container_name[0]);

                //first check to see it has already stopped, if so, no need to stop it
                if(!c.getState().equalsIgnoreCase("exited")) {
                    System.out.println("Stopping container" + container_name[0] + "....");
                    dockerClient.stopContainerCmd(c.getId()).exec();
                }

                //proceed with removing container
                System.out.println("Removing container" + container_name[0] + "....");
                dockerClient.removeContainerCmd(c.getId()).exec();

                //recursively check again if the container has now been removed
                ifExistsRemoveContainer(containerName);

            }
        }else{

            System.out.println("[DockerHost: " + dockerHostID.getDockerHostIDAsString() +"] Container: " + containerName + " does not yet exist, or has been removed");

        }

    }

    public void checkContainerStatus(CreateContainerResponse containerResponse){

        InspectContainerResponse inspectContainerResponse = dockerClient.inspectContainerCmd(containerResponse.getId()).exec();
        System.out.println("The status of container:" + inspectContainerResponse.getName() +  " is: " + inspectContainerResponse.getState().getStatus());


    }


    /*******************************************************************************
     *
     * Docker image management
     *
     *******************************************************************************/

    public boolean isDockerTopologyRepositoryAndTagSet() {
        return isDockerTopologyRepositoryAndTagSet;
    }

    public String getTopologyDockerRepositoryAndTag() {
        return topologyDockerRepositoryAndTag;
    }

    public void setTopologyDockerRepositoryAndTag(String topologyDockerRepositoryAndTag) {
        this.topologyDockerRepositoryAndTag = topologyDockerRepositoryAndTag;
        this.isDockerTopologyRepositoryAndTagSet = true;
    }










     //Docker network is already created so no need to create network

     private void createNetwork(){

         // Remove the old network
         List<Network> networks = dockerClient.listNetworksCmd().withNameFilter("edgestream").exec();
         if (!networks.isEmpty()) {
            System.out.println("[DockerHost]: Removing old network");
            dockerClient.removeNetworkCmd("edgestream").exec();
         }

         networks = dockerClient.listNetworksCmd().withNameFilter("edgestream").exec(); //check to see if its removed

         if (networks.isEmpty()) {
            System.out.println("[DockerHost]: Adding new Docker network to this host");
             dockerClient.createNetworkCmd()
            .withName("edgestream")
            .withAttachable(true)
            .withDriver("bridge")
            .exec();
        }
     }




    @Override
    public String toString() {
        return "[DockerHost] {" +
                "dockerHostID=" + dockerHostID +
                ", dockerHostIP='" + dockerHostIP + '\'' +
                ", scheduledHostID='" + scheduledHostID + '\'' +
                ", taskManagerID='" + taskManagerID + '\'' +
                '}';
    }
}
