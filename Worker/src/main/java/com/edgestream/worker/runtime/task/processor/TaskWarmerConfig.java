package com.edgestream.worker.runtime.task.processor;

import com.edgestream.worker.runtime.container.EdgeStreamContainer;
import com.edgestream.worker.runtime.reconfiguration.execution.ExecutionType;
import com.edgestream.worker.runtime.reconfiguration.state.RoutingKey;
import com.edgestream.worker.runtime.task.model.TaskRequestSetupOperator;
import com.edgestream.worker.runtime.task.model.TaskWarmerStrategy;
import com.edgestream.worker.runtime.topology.Topology;
import com.edgestream.worker.runtime.topology.TopologyAttributeAddressManager;
import com.edgestream.worker.runtime.topology.TopologyState;
import org.apache.commons.lang3.tuple.ImmutableTriple;


public class TaskWarmerConfig {

    private final TaskRequestSetupOperator taskRequestSetupOperator;
    private final Topology topology;
    private final PredecessorSuccessorConfig predecessorSuccessorConfig;
    private final RoutingKey routingKey;
    private ImmutableTriple<String,String,String> warmUpReplicaTypeAndIP =  new ImmutableTriple<>("NONE","NONE","NONE"); //ex input "F","OP_F_002","10.70.2.1"
    private final ExecutionType executionType;
    private final String operatorType;
    private EdgeStreamContainer designatedSourceWarmer;



    protected TaskWarmerConfig(TaskRequestSetupOperator taskRequestSetupOperator
            , Topology topology
            , PredecessorSuccessorConfig predecessorSuccessorConfig
            , RoutingKey routingKey) {

        this.taskRequestSetupOperator = taskRequestSetupOperator;
        this.topology = topology;
        this.predecessorSuccessorConfig = predecessorSuccessorConfig;
        this.routingKey = routingKey;
        this.executionType = taskRequestSetupOperator.getExecutionType();
        this.operatorType = taskRequestSetupOperator.getOperatorType();

        configureWarmUpReplicaIP();
        buildWarmerStrategy();
    }




    protected ImmutableTriple<String,String,String> getWarmUpReplicaTypeAndIP() {
        return warmUpReplicaTypeAndIP;
    }



    private ImmutableTriple<String,String,String> configureWarmUpReplicaIP(){

        /****************************************************************************
         * Option 1:
         * check to see if a stateless operator container of the same type already exists, and if so we can use it to warm up this operator.
         *  TODO: this only works for stateful when the datacenter is empty, we need to implement support for stateful operators that are added to an existing topology
         * */
        if(executionType.equals(ExecutionType.RECONFIGURATION)) {  //can only warm up reconfigurations and not the initial deployment.
            if (predecessorSuccessorConfig.containerOperatorTypeAlreadyExists(operatorType)) {
                designatedSourceWarmer = predecessorSuccessorConfig.getCandidateReplicaContainer(operatorType);
                warmUpReplicaTypeAndIP = new ImmutableTriple<>(operatorType,designatedSourceWarmer.getOperator_id(),designatedSourceWarmer.getDockerContainerLocalIP());

                System.out.println("[TaskWarmerConfig]: Found same type candidate warm up replica: " + warmUpReplicaTypeAndIP + " for new operator: " + taskRequestSetupOperator.getOperatorID());
            }
        }else{
            System.out.println("[TaskWarmerConfig]: Initial cloud deployment, operator [" + taskRequestSetupOperator.getOperatorID()  +  "] cannot be warmed up.....");
        }


        /*********************************************************************************
         * Option 2:
         * When a stateless predecessor operator is already installed we can use it to warm up this operator
         *  TODO: this only works for stateful when the datacenter is empty, we need to implement support for stateful operators that are added to an existing topology
         * **/
        if(warmUpReplicaTypeAndIP.getRight().equals("NONE")
                && executionType.equals(ExecutionType.RECONFIGURATION)
                && predecessorSuccessorConfig.hasPredecessors()) {

            String predecessorType = predecessorSuccessorConfig.getPredecessors().get(0);

            if (predecessorSuccessorConfig.containerOperatorTypeAlreadyExists(predecessorType)) { //TODO: only one predecessor is supported
                designatedSourceWarmer = predecessorSuccessorConfig.getCandidateReplicaContainer(predecessorType);
                warmUpReplicaTypeAndIP = new ImmutableTriple<>(predecessorType,designatedSourceWarmer.getOperator_id(),designatedSourceWarmer.getDockerContainerLocalIP());

                System.out.println("[TaskWarmerConfig]: Found a predecessor warm up replica: " + warmUpReplicaTypeAndIP + " for new operator: " + taskRequestSetupOperator.getOperatorID());
            } else {
                System.out.println("[TaskWarmerConfig]: Operator ["+ taskRequestSetupOperator.getOperatorID() +"] has predecessors but they are not yet installed in this data center..");
            }
        }


        /*******************************************
         * Option 3:
         * When a stateless warmup replica has not been found
         * TODO: this only works for stateful when the datacenter is empty, we need to implement support for stateful operators that are added to an existing topology
         * *****************************************************/
        if(warmUpReplicaTypeAndIP.getRight().equals("NONE") && executionType.equals(ExecutionType.RECONFIGURATION)){
            if(routingKey.hasSyncTuple()) {
                System.out.println("[TaskWarmerConfig]: This operator is stateful, we do not warm up stateful operators");
            }else{
                System.out.println("[TaskWarmerConfig]: This operator is stateless but no warm up replica exists for operator [" + taskRequestSetupOperator.getOperatorID()
                        + "], this is a new child data center install, warm up is needed from the broker....");
            }
        }

        return warmUpReplicaTypeAndIP;
    }



    private TaskWarmerStrategy buildWarmerStrategy(){

        TaskWarmerStrategy taskWarmerStrategy = null;

        String operatorAddress = topology.getOperatorAddress(taskRequestSetupOperator.getOperator());
        TopologyAttributeAddressManager topologyAttributeAddressManager = topology.getTopologyOperatorAddressManager(operatorAddress).getTopologyAttributeAddressManager(routingKey);

        /*************************************************************************************************
         *
         *
         * Default Cloud deployment strategy
         *              |
         *              V
         **************************************************************************************************/

        // Option 1:
        if(taskRequestSetupOperator.getExecutionType().equals(ExecutionType.INITIAL)){
            topologyAttributeAddressManager.setAddressToHot(); // cloud so no need to warm up
            taskWarmerStrategy = TaskWarmerStrategy.NO_INITIAL_DEPLOYMENT;
            setTaskWarmerStrategy(taskWarmerStrategy);
        }


        /*************************************************************************************************
         *
         *
         * Strategies for new child data center deployments
         *              |
         *              V
         **************************************************************************************************/
        // Option 2:
        // not a cloud deployment and this attribute address has never been used, so its cold and needs to be warmed up
        if(taskRequestSetupOperator.getExecutionType().equals(ExecutionType.RECONFIGURATION)
                && topologyAttributeAddressManager.isCold()
                && topology.getTopologyState().equals(TopologyState.PAUSED)){

            topologyAttributeAddressManager.installDualRouteToTopologyNoMatch(); //adds a divert this attribute address that copies the tuples to the no match address

            if(predecessorSuccessorConfig.getPredecessorIP().equals("NONE")) {
                taskWarmerStrategy = TaskWarmerStrategy.YES_ACTIVEMQ; // There is no predecessor container in this datacenter, will warm from ActiveMQ broker
            }else{
                // Since the TopologyState is PAUSED the predecessor will warm from broker,
                // so we will not warm up this operator independently, it will be part of a chained warmup
                taskWarmerStrategy = TaskWarmerStrategy.NO_ZMQ_P2P;
            }
            setTaskWarmerStrategy(taskWarmerStrategy);


            //override when this is a stateful operator
            if(routingKey.hasSyncTuple()) {
                setTaskWarmerStrategy(TaskWarmerStrategy.NO_STATEFUL_DUAL_ROUTED);
            }
        }


        /*************************************************************************************************
         *
         * TODO: adding STATEFUL operators to running topologies (TopologyState.RUNNING) in the SAME DC is
         *  currently not support so the following logic (Option 3 and 4) will trigger and set an incorrect
         *  configuration if more than ONE operator is started in the same DC.
         *
         *
         * Strategies for running topologies
         *              |
         *              V
         **************************************************************************************************/
        // Option 3:
        //The topology is already running and we need to add a new operator container to it, but we must warm it from a predecessor first
        if(predecessorSuccessorConfig.hasPredecessors()) {
            if (taskRequestSetupOperator.getExecutionType().equals(ExecutionType.RECONFIGURATION)
                    && topology.getTopologyState().equals(TopologyState.RUNNING)
                    && getWarmUpReplicaTypeAndIP().getLeft().equals(predecessorSuccessorConfig.getPredecessors().get(0))) { //TODO: this assumes a single predecessor, this needs to be changed if we have more than one predecessor

                System.out.println("[TaskWarmerConfig]: Found a predecessor container will attempt to warm from it");
                taskWarmerStrategy = TaskWarmerStrategy.YES_PREDECESSOR_CONTAINER;
                setTaskWarmerStrategy(taskWarmerStrategy);
                taskRequestSetupOperator.setNotifyPredecessorWhenReady(true); //tell this operate to notify its upstream processor that its done warming
                setWaitForSuccessorReadyMessageFlag();
            }
        }

        // Option 4:
        //The topology is already running so we will warm from a container of the same type.
        //This ALSO implies that the predecessor is live and already outputting to ZMQ, so we dont need to send it a ready flag.
        if (taskRequestSetupOperator.getExecutionType().equals(ExecutionType.RECONFIGURATION)
                && topology.getTopologyState().equals(TopologyState.RUNNING)
                && getWarmUpReplicaTypeAndIP().getLeft().equals(operatorType)){

                System.out.println("[TaskWarmerConfig]: Found a container of the same type will attempt to warm from it");

                //This container has no predecessor, so it must connect to the broker after warming from the replica
                if(predecessorSuccessorConfig.getPredecessorIP().equals("NONE")) {
                    taskWarmerStrategy = TaskWarmerStrategy.YES_SAME_CONTAINER_TYPE_SWITCH_AMQ;
                }else{//else there is a predecessor and it must connect directly to it after the warmup. The container should send a switch request to that container just in case its writing its output to the broker
                    taskWarmerStrategy = TaskWarmerStrategy.YES_SAME_CONTAINER_TYPE_SWITCH_ZMQ;
                }

            setWaitForSuccessorReadyMessageFlag();
            setTaskWarmerStrategy(taskWarmerStrategy);
        }

        return taskWarmerStrategy;
    }


    /**
     * This will check to see if there are any successors in this DagPlan and if this operator should wait for them to finish warming up
     */
    private void setWaitForSuccessorReadyMessageFlag(){

        if(taskRequestSetupOperator.hasSuccessorsInDagPlan()){
            taskRequestSetupOperator.setWaitForSuccessorReadyMessage(true);
            System.out.println("[TaskWarmerConfig]:  According to the DagPlan, this operator [WILL] have a successor, set it to wait for a ready message");
        }else{
            System.out.println("[TaskWarmerConfig]:  According to the DagPlan, this operator [WILL NOT] have a successor, leave notification flag set to false");
        }

    }


    private void setTaskWarmerStrategy(TaskWarmerStrategy taskWarmerStrategy){
        taskRequestSetupOperator.setTaskWarmerStrategy(taskWarmerStrategy);
        System.out.println("[TaskWarmerConfig]: " + "Setting warm up flag to ["+ taskRequestSetupOperator.getTaskWarmerStrategy() +"]");
    }
}
