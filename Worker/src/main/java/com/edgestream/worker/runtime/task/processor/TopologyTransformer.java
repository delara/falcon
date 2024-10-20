package com.edgestream.worker.runtime.task.processor;

import com.edgestream.worker.runtime.reconfiguration.execution.ExecutionType;
import com.edgestream.worker.runtime.reconfiguration.state.RoutingKey;
import com.edgestream.worker.runtime.task.TaskDriver;
import com.edgestream.worker.runtime.task.TaskMigration;
import com.edgestream.worker.runtime.task.TaskWarmer;
import com.edgestream.worker.runtime.task.model.TaskRequestSetupOperator;
import com.edgestream.worker.runtime.task.model.TaskWarmerStrategy;
import com.edgestream.worker.runtime.topology.Topology;
import com.edgestream.worker.runtime.topology.TopologyAttributeAddressManager;
import com.edgestream.worker.runtime.topology.TopologyState;

public class TopologyTransformer {

    private final Topology topology;
    private final TaskRequestSetupOperator taskRequestSetupOperator;
    private final RoutingKeyConfig routingKeyConfig;
    private final String operatorInputFQQN;
    private final TaskDriver taskDriver;


    protected TopologyTransformer(
            Topology topology
            , TaskRequestSetupOperator taskRequestSetupOperator
            , RoutingKeyConfig routingKeyConfig
            , String operatorInputFQQN
            , TaskDriver taskDriver) {

        this.topology = topology;
        this.taskRequestSetupOperator = taskRequestSetupOperator;
        this.routingKeyConfig = routingKeyConfig;
        this.operatorInputFQQN = operatorInputFQQN;
        this.taskDriver = taskDriver;
    }



    private void updateTopologyState(Topology topology
            , TopologyAttributeAddressManager topologyAttributeAddressManager
            , RoutingKey routingKey
            , String reconfigurationPlanID
            , TaskRequestSetupOperator taskRequestSetupOperator){

        /********************************************************************************
         * OPTION 1:
         *  A NEW topology is instantiated into a TopologyState.PAUSED state.
         *  This topology is NEW in this datacenter so we need to check if we are ready to divert tuples into this data center.
         *  This entails checking to see if the OperatorSetupQueue is empty, meaning that all operators are online and
         *  ready to accept tuples.
         ********************************************************************************/

        //The topology is always PAUSED when it is first created then it gets set to RESTART_REQUIRED.
        //TODO: if we remove operators in the future, then the topology may need to be set back to PAUSED or removed entirely
        if(topology.getTopologyState() == TopologyState.PAUSED) {
            //there are no colocated operators so just start the topology
            if (taskRequestSetupOperator.getColocatedOperators().equalsIgnoreCase("")) {
                try {
                    //1. if dual routing is needed need to pause the attribute address first
//                    if (routingKey.hasSyncTuple() && topologyAttributeAddressManager.hasDualRoutingEnabled()){
//                        sendMarkerTuple(topologyAttributeAddressManager,routingKey);
//
//                    }
                    topology.setTopologyState(TopologyState.RESTART_REQUIRED);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }else{
                if(topology.getTopologyReconfigurationManager().isOperatorSetupQueueEmpty(reconfigurationPlanID)){ //check if there are no more operators to be setup, if so we are ready to restart the topology.
                    try {
                        topology.setTopologyState(TopologyState.RESTART_REQUIRED);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }else{ //There are more operators to setup after this one, so don't set the TopologyState.RESTART_REQUIRED
//                    if (routingKey.hasSyncTuple() && topologyAttributeAddressManager.hasDualRoutingEnabled()){
//                        sendMarkerTuple(topologyAttributeAddressManager,routingKey);
//                    }
                }
            }
        }


        /********************************************************************************
         * OPTION 2:
         *  A replica container was added to an existing active attribute address so no
         *  change is needed on the broker side.
         ********************************************************************************/
        if(topology.getTopologyState() == TopologyState.RUNNING) {
            System.out.println("[TopologyTransformer]: Replica was added, no need to restart Topology....");
        }

        /********************************************************************************
         * OPTION 3:
         *  An operator container was added to the broker and those queues/addresses need to be connected to the
         *  main tuple flow.
         ********************************************************************************/
        if(topology.getTopologyState() == TopologyState.RESTART_REQUIRED) {
            try {
                checkIfTuplesCanFlow(topology,reconfigurationPlanID); // This will block until all operators in this reconfiguration plan report back as READY
                taskDriver.getTaskBrokerConfig().addDivertToDefaultTopology(topology);
                topology.resumeAllOperatorAndAttributeAddresses(); //this allows the tuples to flow to all the operator and attribute addresses at once.
                topology.setTopologyState(TopologyState.RUNNING);
                System.out.println("[TopologyTransformer]: Topology: " + topology.getTopologyID() + " was started/restarted...");

            } catch (Exception e) {
                e.printStackTrace();
            }

        }

        if (routingKey.hasSyncTuple() && topologyAttributeAddressManager.hasDualRoutingEnabled()){
            sendMarkerTuple(topologyAttributeAddressManager,routingKey);
        }


    }

    protected void transform(){

        String operatorAddress = topology.getOperatorAddress(taskRequestSetupOperator.getOperator());
        TopologyAttributeAddressManager topologyAttributeAddressManager = topology.getTopologyOperatorAddressManager(operatorAddress).getTopologyAttributeAddressManager(routingKeyConfig.getRoutingKey());

        //1 .First check to see if the topology needs to be restarted.
        updateTopologyState(topology,topologyAttributeAddressManager,routingKeyConfig.getRoutingKey(),taskRequestSetupOperator.getReconfigurationPlanID(),taskRequestSetupOperator);

        /***************************************************************************************************************
         * Stateful Task Migration Logic:
         *
         * A marker tuple was injected above in method [updateTopologyState()] for new single operators running in a datacenter.
         * Now we need to monitor that queue and then remove the dual routing
         * ************************************************************************************************************/
        if(routingKeyConfig.getRoutingKey().hasSyncTuple()){

            System.out.println("[TopologyTransformer]: Migration required....Starting migration manager........");
            String taskMigrationID = taskRequestSetupOperator.getOperator().getOperatorID().getOperatorID_as_String()+ "_migrated";
            String[] addressParts = operatorInputFQQN.split("::");
            String localAddressToMonitor = addressParts[0];
            String localQueueToMonitor = addressParts[1];
            TaskMigration taskMigration = new TaskMigration(taskMigrationID,localQueueToMonitor,localAddressToMonitor, topology.getTaskMigrationManager(),topologyAttributeAddressManager);

            //This starts the TaskMigration thread to check the queue backlog. If the backlog goes below 100, then the dual routing is stopped.
            topology.getTaskMigrationManager().addAndTriggerMigration(taskMigration);
        }


        /***************************************************************************************************************
         * Stateless Task Migration Logic:
         **************************************************************************************************************/

        /**
         * Option 1: The stateless operator will warm from the broker and so a TaskWarmer needs to be scheduled but
         * NOT yet started because there may be other operators that are part of this reconfiguration plan and
         * we need to wait for them to come online.
         **/
        if(taskRequestSetupOperator.getTaskWarmerStrategy().equals(TaskWarmerStrategy.YES_ACTIVEMQ)
                && !routingKeyConfig.getRoutingKey().hasSyncTuple()){
            System.out.println("[TopologyTransformer]: Warm up required, creating warmup manager.......");
            String taskWarmerID = taskRequestSetupOperator.getOperator().getOperatorID().getOperatorID_as_String()+ "_warmer";
            String[] addressParts = operatorInputFQQN.split("::");
            String localAddressToMonitor = addressParts[0];
            String localQueueToMonitor = addressParts[1];
            TaskWarmer taskWarmer = new TaskWarmer(taskWarmerID,localQueueToMonitor,localAddressToMonitor, topology.getTaskWarmerManager(),topologyAttributeAddressManager,taskRequestSetupOperator.getReconfigurationPlanID());

            topology.getTaskWarmerManager().addFutureWarmUpTask(taskWarmer);
        }


        /**
         * Option 2: This operator is the last one that will be setup in this reconfiguration plan(because the setup queue is now empty)
         * So we can now trigger the warm up for the whole chain.
         */
        if(topology.getTopologyReconfigurationManager().isOperatorSetupQueueEmpty(taskRequestSetupOperator.getReconfigurationPlanID())
            && topology.getTopologyState() == TopologyState.RUNNING
            && taskRequestSetupOperator.getExecutionType() == ExecutionType.RECONFIGURATION
            && !routingKeyConfig.getRoutingKey().hasSyncTuple()){
            //This starts the TaskWarmer thread to wait for 120 seconds, then the dual routing is stopped.
            //start the warmer now because no more operators in the chain
            topology.getTaskWarmerManager().triggerWarmUp(taskRequestSetupOperator.getReconfigurationPlanID());
        }

        /**
         * Option 3: This operator warms up from the other containers, so that container will go through an
         * [autonomous warm up] process and this Worker will not participate in that process. ie nothing to do here.
         */
        if(taskRequestSetupOperator.getTaskWarmerStrategy() == TaskWarmerStrategy.YES_PREDECESSOR_CONTAINER
            || taskRequestSetupOperator.getTaskWarmerStrategy() == TaskWarmerStrategy.YES_SAME_CONTAINER_TYPE_SWITCH_AMQ){
            System.out.println("[TopologyTransformer]: Container will warm up via ZMQ");
        }
    }






    /**
     *  This sends the marker tuple into the attribute address. The address should be paused otherwise the tuple will
     *  be immediately sent to the queues on the attribute address. We want to tuple to stay im the address and then fill
     *  in the tuples from the operator address behind the marker.
     * @param topologyAttributeAddressManager
     * @param routingKey
     */
    private void sendMarkerTuple(TopologyAttributeAddressManager topologyAttributeAddressManager, RoutingKey routingKey){
        System.out.println("[TopologyTransformer] Sending marker tuple to trigger backup process........");
        topologyAttributeAddressManager.beginStatefulOperatorBackupProcess(routingKey.getSyncTuple());

    }

    private void checkIfTuplesCanFlow(Topology topology, String reconfigurationPlanID) throws Exception {

        boolean confirmationReceived= false;

        while(!confirmationReceived) {
            if (topology.getTopologyReconfigurationManager().isOperatorConfirmationQueueEmpty(reconfigurationPlanID)) {

                System.out.println("[TopologyTransformer]: Tuples can flow");
                confirmationReceived = true;

            }else{
                System.out.println("[TopologyTransformer]: Operator confirmation queue not yet empty, waiting for 5 seconds and check again...");
                Thread.sleep(5000);
            }
        }

    }

}
