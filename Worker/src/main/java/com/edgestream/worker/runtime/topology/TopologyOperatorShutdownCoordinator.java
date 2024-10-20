package com.edgestream.worker.runtime.topology;

import com.edgestream.worker.common.DockerHostIDFormatter;
import com.edgestream.worker.operator.Operator;
import com.edgestream.worker.runtime.container.EdgeStreamContainer;
import com.edgestream.worker.runtime.docker.DockerHost;
import com.edgestream.worker.runtime.docker.DockerHostID;
import com.edgestream.worker.runtime.task.TaskDriver;
import com.edgestream.worker.runtime.task.broker.TaskBrokerConfig;
import com.github.dockerjava.api.DockerClient;
import org.apache.commons.lang3.tuple.Pair;

public class TopologyOperatorShutdownCoordinator {


    private final TaskBrokerConfig taskBrokerConfig;
    private final DockerClient dockerClient;
    private final DockerHost dockerHost;
    private final Topology topology;
    private final String operatorInputType;


    public TopologyOperatorShutdownCoordinator(Topology topology, String operatorInputType, TaskDriver taskDriver) {

        this.topology = topology;
        this.operatorInputType = operatorInputType;
        this.taskBrokerConfig = taskDriver.getTaskBrokerConfig();

        //TODO: need to replace these nulls with the real ip of the docker host that we want to shutdown containers
        this.dockerHost = taskDriver.getDockerHostManager().getDockerHost(new DockerHostID(DockerHostIDFormatter.formatDockerHostID(null)));
        this.dockerClient = taskDriver.getDockerHostManager().getDockerHost(new DockerHostID(DockerHostIDFormatter.formatDockerHostID(null))).getDockerClient();


    }

    public void shutdownOperator(){

        //1. Find a candidate operator to shutdown
        Operator operatorToRemove = findCandidateOperator(operatorInputType);

        removeOperatorFromTopologyOperatorAndAddressList(operatorToRemove);
        removeOperatorFromTopology(operatorToRemove);

        //2. Need to check to see if the operator address needs to be removed if this is the
        // last operator instance on that address
        if(isLastOperatorTypeInDataCenter(operatorToRemove.getInputType())) {

            removeOperatorTypeFromDatacenter(operatorToRemove);
        }

        //4. Pause the operator address
        pauseOperatorAddress(operatorToRemove);

        //5. Find the edge stream container and remove it
        edgeStreamContainerCleanUp(operatorToRemove);

        resumeOperatorAddress(operatorToRemove);

    }



    private boolean isLastOperatorTypeInDataCenter(String operatorType){

        return !topology.isOperatorTypeInstalled(operatorType);

    }

    //TODO: Fix the removal process
    private void edgeStreamContainerCleanUp(Operator operator){

       /* //1. Find the edgestream container
        if(dockerHost.EdgeStreamContainerExists(operator)){

            EdgeStreamContainer edgeStreamContainer = dockerHost.findEdgeStreamContainerByOperatorID(operator);

            this.dockerHost.removeEdgeStreamContainer(edgeStreamContainer);
        }else{

            System.out.println("Could not find Edgestream container to remove");

        }*/

    }


    /**
     * This function is only safe to call when we have determined that this operator type has no replicas in this data center
     * @param operator
     */
    private void removeOperatorTypeFromDatacenter(Operator operator){

        //1. Pause the topology address
        pauseTopologyAddress();

        // TODO: if some kind of synchronsation is needed, this the point where you would inject the marker tuple

        //2a. Update and rebuild the catch all divert on the Topology Address so that tuples
        // of this type are now "caught" and are pushed up in the parent datacenter. This is a negative filter
        // so the logic is to divert everything that is not of a particular type. For example, NOT (tupleType='C') AND NOT (tupleType='E')
        updateCatchAllAddress(operator);

        //2b. Because tuples are evaluated against all matching diverts, we need to remove the divert that sends tuples to the operator addresses in this datacenter.
        // other wise tuples will continue to be routed to this datacenter and get pushed up because a tuple will meet condition of both diverts.
        removeDivertTypeFromTopologyAddress(operator);

        //2b. Resume the Topology address
        resumeTopologyAddress();

    }



    private void removeOperatorFromTopologyOperatorAndAddressList(Operator operatorToRemove) {

        for (Pair<Operator,String> p : topology.getOperatorAndAddressList()){

            if (p.getKey() == operatorToRemove){
                topology.getOperatorAndAddressList().remove(p);
                break;
            }

        }

    }



    private Operator findCandidateOperator(String operatorInputType){

        return this.topology.findInstalledOperatorByType(operatorInputType);

    }

    private void removeOperatorFromTopology(Operator operator){

        this.topology.uninstallOperator(operator);

    }


    /**************************************************************************************
     *
     *
     * Broker Address Controllers
     *
     *****************************************************************************************/






    /**
     * This removes the operator divert type from the  Primary Topology address
     * @param operator
     */
    private void removeDivertTypeFromTopologyAddress(Operator operator) {

        String divertName = topology.getTopologyID() + "_" + operator.getInputType() + "_divert";
        taskBrokerConfig.destroyDivert(divertName);


    }


    private void resumeTopologyAddress(){

        try {
            this.taskBrokerConfig.getAddressController(topology.getPrimaryTopologyAddress()).resume();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    private void pauseTopologyAddress(){
        try {
            this.taskBrokerConfig.getAddressController(this.topology.getPrimaryTopologyAddress()).pause();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    private void pauseOperatorAddress(Operator operator){

        try {
            this.taskBrokerConfig.getAddressController(operator.getOperatorAddress()).pause();
        } catch (Exception e) {
            e.printStackTrace();
        }


    }

    private void resumeOperatorAddress(Operator operator){

        try {
            this.taskBrokerConfig.getAddressController(operator.getOperatorAddress()).resume();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    /**
     * This function modifies the "primary_divert only which is responsible for pushing tuples to the no match address"
     * This does not remove the individual operator TYPE diverts when need to be removed also
     * @param operator
     */
    private void updateCatchAllAddress(Operator operator){


        //1. Destroy the old catch all divert
        try {
            this.taskBrokerConfig.destroyCatchAllDivert(this.topology);
        } catch (Exception e) {
            e.printStackTrace();
        }

        //2. update the filter expression for the divert. This will now force tuples of this type to be pushed to the no match address
        this.topology.removeOperatorDivertFromTopologyAddress("(tupleType='" + operator.getInputType()+ "')");

        //3. rebuild the divert on the no match address.
        try {
            this.taskBrokerConfig.createDivertOnAddress(topology,topology.getPrimary_catch_all_divert_name(),"push_to_no_match_address",topology.getPrimary_catch_all_divert_filter_expression());
        } catch (Exception e) {
            e.printStackTrace();
        }


    }




}
