package com.edgestream.worker.runtime.docker;

import com.edgestream.worker.common.DockerHostIDFormatter;
import com.edgestream.worker.config.EdgeStreamGetPropertyValues;
import com.edgestream.worker.operator.Operator;
import com.edgestream.worker.runtime.container.EdgeStreamContainer;
import com.edgestream.worker.runtime.docker.exception.EdgeStreamDockerException;
import com.edgestream.worker.runtime.scheduler.ScheduledSlot;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;

public class DockerHostManager {


    private final String dockerHostManagerID;
    private final String metricsDefaultAddress;
    private final String dockerRemoteAPIPort = "2375";
    private final String brokerIP;


    private final List<DockerHost> dockerHosts = new ArrayList<>();
    //private CopyOnWriteArrayList<EdgeStreamContainer> activeEdgeStreamContainers = new CopyOnWriteArrayList<>();
    private final HashMap<String, ArrayDeque<EdgeStreamContainer>> operatorContainerScheduler = new HashMap<>();




    public DockerHostManager(String dockerHostManagerID, String metricsDefaultAddress, String taskManagerID,String brokerIP) {
        this.dockerHostManagerID = dockerHostManagerID;
        this.metricsDefaultAddress = metricsDefaultAddress;
        this.brokerIP = brokerIP;
        System.out.println("[DockerHostManager] Docker host manager created: " + dockerHostManagerID);

        try {
            System.out.println("[DockerHostManager] adding hosts........");

            addDockerHosts(taskManagerID);

        } catch (IOException e) {
            e.printStackTrace();
        }


    }


    /***************************************************************************************************************
     *
     * At boot time we populate the list of hosts defined in the properties file. This method only gets called once by the constructor of this class
     *
     ************************************************************************************************************/
    private void addDockerHosts(String taskManagerID) throws IOException {

        Iterator dockerHostInfo = EdgeStreamGetPropertyValues.getListOfDatacenterDockerHosts(taskManagerID).entrySet().iterator();

        while(dockerHostInfo.hasNext()) {
            Map.Entry mapElement = (Map.Entry)dockerHostInfo.next();

            String dockerHostIP = (String)mapElement.getKey();
            String HostID = (String)mapElement.getValue();

            DockerHostID dockerHostID = new DockerHostID(DockerHostIDFormatter.formatDockerHostID(dockerHostIP));
            DockerHost dockerHost = new DockerHost(dockerHostID,taskManagerID,brokerIP,dockerHostIP,HostID, dockerRemoteAPIPort,metricsDefaultAddress);
            dockerHosts.add(dockerHost);

        }

        printDockerHostList();

    }

    private void printDockerHostList(){
        System.out.println("[DockerHostManager]: --------List of all docker hosts that have been setup in this DC");
        for(DockerHost dockerHost :dockerHosts){
            System.out.println("[DockerHostManager]: Docker Host= " + dockerHost.getDockerHostID().getDockerHostIDAsString());
        }
    }




    public DockerHostID scheduleDockerHost(ScheduledSlot scheduledSlot){

        //Step 1. Use the information in the scheduledSlot object to find the DockerHostID
        String taskManagerID = scheduledSlot.getTaskManagerID();
        String scheduledHostID = scheduledSlot.getScheduledHostID();

        System.out.println("[DockerHostManager]: attempting to find scheduledSlot :" + scheduledSlot);

        DockerHostID hostIDToFind = null;

        for (DockerHost dockerHost: dockerHosts){
            if(dockerHost.getTaskManagerID().equalsIgnoreCase(taskManagerID) && dockerHost.getScheduledHostID().equalsIgnoreCase(scheduledHostID)){

                hostIDToFind = dockerHost.getDockerHostID();
                System.out.println("[DockerHostManager]: Found dockerHostID: " +hostIDToFind.getDockerHostIDAsString());
            }
        }

        if (hostIDToFind == null ){
            throw new EdgeStreamDockerException("[DockerHostManager] Cloud not find a scheduledHostID : [" +  scheduledHostID+ "]", new Throwable());
        }

        return hostIDToFind;
    }


    public DockerHost getDockerHost(DockerHostID dockerHostID){

        for (DockerHost dockerHost : dockerHosts){
            if (dockerHost.getDockerHostID().getDockerHostIDAsString().equalsIgnoreCase(dockerHostID.getDockerHostIDAsString())){
                return dockerHost;
            }else{
                throw new EdgeStreamDockerException("Could not find a Docker host with ID: " +  dockerHostID.getDockerHostIDAsString(), new Throwable());
            }
        }

        return null;
    }



    private void remove(String dockerHostToBeRemoved){

        for (DockerHost dockerHost : dockerHosts){
            if (dockerHost.getDockerHostID().getDockerHostIDAsString().equals(dockerHostToBeRemoved)){
                dockerHosts.remove(dockerHost);
            }else{
                throw new EdgeStreamDockerException("Could not find a Docker host with ID: " +  dockerHostToBeRemoved, new Throwable());
            }
        }
    }




    /*************************************************************************************************************
     *
     *                                  Container Management Functionality
     *                                              |
     *                                              |
     *                                              V
     *
     * **********************************************************************************************************/


    /**
     * This function adds the container to the scheduler. The scheduler is a simple round robin policy.
     * TODO: This function is temporary until we implement consul to handle the load balancing
     * @param edgeStreamContainerToBeAdded
     */
    private void addEdgeStreamContainerToScheduler(EdgeStreamContainer edgeStreamContainerToBeAdded){

        //The first time this operator type has been scheduled, so the type will not exist yet
        if(!operatorContainerScheduler.containsKey(edgeStreamContainerToBeAdded.getOperatorInputType())){
            //1. Create a new queue to hold the containers
            operatorContainerScheduler.put(edgeStreamContainerToBeAdded.getOperatorInputType(), new ArrayDeque<>());
            //2. Get that new dequeue and add this container to it

            ArrayDeque<EdgeStreamContainer> containerScheduler = operatorContainerScheduler.get(edgeStreamContainerToBeAdded.getOperatorInputType());
            System.out.println("A new EdgeStream schedule has been create for operator Type: "+ edgeStreamContainerToBeAdded.getOperatorInputType());
            containerScheduler.addLast(edgeStreamContainerToBeAdded);
            System.out.println("EdgeStream Container: " + edgeStreamContainerToBeAdded.getEdgeStreamContainerID().getEdgeStreamContainerIDAsString()
                    + " has been added to the schedule");

        }
        //This operator type already has a scheduler so just add this container to the back of the list
        else{
            ArrayDeque<EdgeStreamContainer> containerScheduler = operatorContainerScheduler.get(edgeStreamContainerToBeAdded.getOperatorInputType());
            containerScheduler.addLast(edgeStreamContainerToBeAdded);
        }
    }

    public void addEdgeStreamContainer(EdgeStreamContainer edgeStreamContainerToBeAdded, DockerHost dockerHost){
         dockerHost.addEdgeStreamContainer(edgeStreamContainerToBeAdded); //add to the host so that the metrics collector can monitor it
         addEdgeStreamContainerToScheduler(edgeStreamContainerToBeAdded); //add to the scheduler to be used in load balancing decisions

    }





    /******************************************************************************
     * This method generates a  list of ALL containers in the datacenter
     * @return
     ***********************************************************************************/
    public List<EdgeStreamContainer>  getActiveEdgeStreamContainers(){

        ArrayList<EdgeStreamContainer> allContainersInDC = new ArrayList<>();

        for (DockerHost host: dockerHosts){
            List<EdgeStreamContainer>  hostContainerList  = host.getActiveEdgeStreamContainers();
            for (EdgeStreamContainer es : hostContainerList){
                allContainersInDC.add(es);
            }
        }

        return allContainersInDC;

    }


    /***************************************************************************************
     * This function searches all docker hosts gets the next container for a specific operator type.
     *
     * @param operatorType
     * @return
     ******************************************************************************************/
    public EdgeStreamContainer getNextEdgeStreamContainerInSchedule(String operatorType){

        EdgeStreamContainer edgeStreamContainer;
        //1. get the container from the scheduler
        edgeStreamContainer = operatorContainerScheduler.get(operatorType).pollFirst();
        //2. put it to the back of the list, because it was removed during the deque
        operatorContainerScheduler.get(operatorType).addLast(edgeStreamContainer);
        //3. return the container
        return  edgeStreamContainer;
    }

    public EdgeStreamContainer getEdgeStreamContainerReplica(String operatorType){

        EdgeStreamContainer edgeStreamContainer;
        //1. get the container from the scheduler
        edgeStreamContainer = operatorContainerScheduler.get(operatorType).peekLast(); //peek to avoid removing container from Array Deque

        //3. return the container
        return  edgeStreamContainer;
    }




    /***
     * Removing an {@link EdgeStreamContainer} involves shutting down the actual container on the Docker host machine
     * and also removing the object reference from the  list.
     */
    public void removeEdgeStreamContainer(EdgeStreamContainer edgeStreamContainerToBeRemoved){

        //1. remove the container from the main list
        findAndRemoveActiveEdgeStreamContainer(edgeStreamContainerToBeRemoved);

        //2. remove the container from the scheduler
        removeContainerFromScheduler(edgeStreamContainerToBeRemoved);

        //3. remove the physical container from docker
        findAndRemoveContainerFromDockerHost(edgeStreamContainerToBeRemoved.getEdgeStreamContainerID().getDockerContainerName());

        //4. Print an update list of active containers
        printALLEdgeStreamContainersInDC();

    }


    private void findAndRemoveActiveEdgeStreamContainer(EdgeStreamContainer edgeStreamContainerToBeRemoved){

       for (DockerHost host: dockerHosts){
            List<EdgeStreamContainer>  hostContainerList  = host.getActiveEdgeStreamContainers();
            for (EdgeStreamContainer es : hostContainerList){
                if(es.getEdgeStreamContainerID() == edgeStreamContainerToBeRemoved.getEdgeStreamContainerID()){
                    host.removeEdgeStreamContainer(edgeStreamContainerToBeRemoved);
                }
            }
        }
    }


    private void findAndRemoveContainerFromDockerHost(String containerName){

        for (DockerHost host: dockerHosts){
            List<EdgeStreamContainer>  hostContainerList  = host.getActiveEdgeStreamContainers();
            for (EdgeStreamContainer es : hostContainerList){
                if(es.getEdgeStreamContainerID().getDockerContainerName() == containerName){
                    host.ifExistsRemoveContainer(containerName);
                }
            }
        }
    }

    private void printALLEdgeStreamContainersInDC(){

        for (DockerHost host: dockerHosts){
            host.printEdgeStreamContainerList();
        }
    }


    private void removeContainerFromScheduler(EdgeStreamContainer edgeStreamContainer){
        ArrayDeque<EdgeStreamContainer> containerScheduler = operatorContainerScheduler.get(edgeStreamContainer.getOperatorInputType());
        containerScheduler.remove(edgeStreamContainer);

        System.out.println("EdgeStream Container: " + edgeStreamContainer.getEdgeStreamContainerID().getEdgeStreamContainerIDAsString()
                + " has been REMOVED from the scheduler");

    }



    public boolean EdgeStreamContainerExists(Operator operator){


        boolean containerFound = false;
        for (EdgeStreamContainer edgeStreamContainer : getActiveEdgeStreamContainers()){

            if(edgeStreamContainer.getOperator_id().equals(operator.getOperatorID().getOperatorID_as_String())){
                System.out.println("Container found for operator ID: " +  edgeStreamContainer.getOperator_id());
                containerFound = true;
            }
        }

        return containerFound;

    }

    public EdgeStreamContainer findEdgeStreamContainerByOperatorID(Operator operator){

        EdgeStreamContainer containerToFind= null;

        for (EdgeStreamContainer edgeStreamContainer : getActiveEdgeStreamContainers()){

            if(edgeStreamContainer.getOperator_id().equals(operator.getOperatorID().getOperatorID_as_String())){
                containerToFind = edgeStreamContainer;
            }
        }
        return  containerToFind;
    }






    /***********************************************************
     *
     * Searches ALL docker hosts in this datacenter and remove the container that matches the dockerContainerName
     * @param dockerContainerName
     *******************************************************************/
    public void findAndRemoveContainerFromDC(String dockerContainerName){

        for (DockerHost dockerHost : dockerHosts){
            dockerHost.ifExistsRemoveContainer(dockerContainerName);
        }

    }











}
