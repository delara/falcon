package com.edgestream.worker.runtime.task.processor;


import com.edgestream.worker.config.EdgeStreamGetPropertyValues;
import com.edgestream.worker.operator.OperatorExternal;
import com.edgestream.worker.operator.OperatorID;
import com.edgestream.worker.runtime.scheduler.ScheduledSlot;
import com.edgestream.worker.runtime.task.TaskRunnerExternal;
import com.edgestream.worker.runtime.task.model.TaskRequestSetupOperator;
import com.edgestream.worker.runtime.docker.*;
import com.edgestream.worker.runtime.task.TaskDriver;
import com.edgestream.worker.runtime.topology.Topology;

import com.github.dockerjava.api.DockerClient;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.SerializationUtils;


import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;

public class TaskRequestProcessorContainer {

    private final TaskRunnerExternal taskRunnerExternal;
    private final TaskDriver taskDriver;
    private final DockerHostID dockerHostID;


    public TaskRequestProcessorContainer(ClientMessage taskRequestMessage, TaskDriver taskDriver) throws Exception {

        this.taskDriver = taskDriver;
        String jarDirectory = EdgeStreamGetPropertyValues.getTASK_MANAGER_WORKING_FOLDER();

        /**************************************************************************************************************
         *
         *                             Task Request Configs Extraction
         *
         *
         * ***********************************************************************************************************/

        String taskManagerID = taskRequestMessage.getStringProperty("Task_Manager_ID");

        TaskRequestSetupOperator taskRequestSetupOperator = new TaskRequestSetupOperator();

        taskRequestSetupOperator.setReconfigurationPlanID(taskRequestMessage.getStringProperty("reconfigurationPlanID"));
        taskRequestSetupOperator.setTopologyID(taskRequestMessage.getStringProperty("Topology"));
        taskRequestSetupOperator.setExecutionType(SerializationUtils.deserialize((byte[]) taskRequestMessage.getObjectProperty("executionType")));
        taskRequestSetupOperator.setOperatorPredecessors(SerializationUtils.deserialize((byte[]) taskRequestMessage.getObjectProperty("OperatorPredecessorType")));
        taskRequestSetupOperator.setOperatorSuccessors(SerializationUtils.deserialize((byte[]) taskRequestMessage.getObjectProperty("OperatorSuccessorType")));
        taskRequestSetupOperator.setColocatedOperators(taskRequestMessage.getStringProperty("colocatedOperators"));
        taskRequestSetupOperator.setDagPlan(SerializationUtils.deserialize((byte[]) taskRequestMessage.getObjectProperty("dagPlan")));

        OperatorExternal operatorExternal = new OperatorExternal(null, new OperatorID(taskRequestMessage.getStringProperty("OperatorID")), taskRequestMessage.getStringProperty("inputType"));
        taskRequestSetupOperator.setOperator(operatorExternal);


        taskRequestSetupOperator.setOperatorID(taskRequestSetupOperator.getOperator().getOperatorID().getOperatorID_as_String()); //this value is preset by the cluster manager so we can get them from the Operator object
        taskRequestSetupOperator.setOperatorType(taskRequestSetupOperator.getOperator().getInputType()); //this value is preset by the cluster manager so we can get them from the Operator object
        taskRequestSetupOperator.setOperatorClassName(taskRequestSetupOperator.getOperator().getClass().getCanonicalName());
        taskRequestSetupOperator.setTask_id(taskRequestSetupOperator.getOperator().getOperatorID().getOperatorID_as_String()); //TODO: need to have cluster manager generate a unique task ID
        taskRequestSetupOperator.setTaskRunnerID(taskRequestSetupOperator.getOperator().getOperatorID().getOperatorID_as_String()); //TODO: if we get a unique task ID then we can generate a task runner ID
        taskRequestSetupOperator.setBroker_ip(taskDriver.getTaskBrokerConfig().getBroker_ip()); //The IP is set by the task manager so we use that, this may change in the future
        taskRequestSetupOperator.setDockerFileType(DockerFileType.valueOf(taskRequestMessage.getStringProperty("dockerFileType")));
        taskRequestSetupOperator.setDag(SerializationUtils.deserialize((byte[]) taskRequestMessage.getObjectProperty("dag")));

        ScheduledSlot scheduledSlot = taskRequestSetupOperator.getDagPlan().getScheduledSlot(taskRequestSetupOperator.getOperator().getOperatorID());

        dockerHostID = taskDriver.getDockerHostManager().scheduleDockerHost(scheduledSlot); // This reference will be used to complete the rest of the setup process

        /*****************************************
         * Stateful Operator Configuration parser
         ****************************************/

        taskRequestSetupOperator.setDagElementType(SerializationUtils.deserialize((byte[]) taskRequestMessage.getObjectProperty("DagElementType")));
        taskRequestSetupOperator.setHasBootstrapState(taskRequestMessage.getBooleanProperty("hasBootstrapState"));

        if(taskRequestSetupOperator.hasBootstrapState()){
            taskRequestSetupOperator.setStateTransferPlan(SerializationUtils.deserialize((byte[]) taskRequestMessage.getObjectProperty("stateTransferPlan")));
        }

        System.out.println(taskRequestSetupOperator);



        /******  Task construction begins here  *****************************************************************/
        /**Topology Setup **/
        //check to see if topology already exists
        boolean existingTopology = taskDriver.topologyAlreadyExists(taskRequestSetupOperator.getTopologyID());

        Topology topology;

        if (!existingTopology) { //topology does not yet exist, we need to create it. We also need to create a bridge to the parent
            topology = new Topology(taskRequestSetupOperator.getTopologyID(),taskDriver);
            taskDriver.addTopology(topology);


            taskDriver.getTaskBrokerConfig().addTopologyAddress(topology);//sets up the PrimaryTopologyAddress address for this topology and the NoMatchTopologyAddress

            if(!taskManagerID.equalsIgnoreCase("W001")) {
                int bridgeCount = taskDriver.getNumberOfBridges();
                System.out.println("[TaskRequestProcessorContainer]: Will create this many Artemis bridges [" + bridgeCount + "]");
                int counter = 0;
                while (counter < bridgeCount) {
                    taskDriver.getTaskBrokerConfig().setUpTopologyBridge(topology, counter);
                    counter++;
                }
            }else{
                System.out.println("[TaskRequestProcessorContainer]: This is the ROOT datacenter, will not create bridges");
            }

            taskDriver.printActiveTopologyList();

        } else {
            topology = taskDriver.getTopology(taskRequestSetupOperator.getTopologyID());  //topology already exists, search for it
        }

        //Add reconfiguration ID and dag
        topology.getTopologyReconfigurationManager().addReconfigurationRequest(taskRequestSetupOperator.getReconfigurationPlanID(),taskRequestSetupOperator.getDagPlan());



        /***************************************************************************************************
        *
        *                                  Address and Queue Configuration
        *
        * ****************************************************************************************************/

        //String operatorOutputAddress = topology.getPrimaryTopologyAddress();  //get the output address for this operator
        //changed APR 30 2021: operators in the same datacenter will never communicate through the broker
        //so only point-to-point via ZMQ so write directly to the no match address
        String operatorOutputAddress = topology.getNoMatchTopologyAddress();
        String operatorOutputBrokerIP = taskRequestSetupOperator.getBroker_ip();  //get the output broker ip for this operator (same as the input broker, because we use bridges to push tuples)
        String operatorInputBrokerIP = taskRequestSetupOperator.getBroker_ip();  //get the input broker ip for this operator (same as the output broker, because we use bridges to push tuples)
        String operatorInputFQQN = taskDriver.getTaskBrokerConfig().setupNewOperatorAddressAndQueue(topology, taskRequestSetupOperator);

        //If jar file has been created then no need to create it again

        if(!topology.isTopologyJarBuilt()) {
           createTopologyJarFromActiveMQMessage(taskRequestMessage, taskRequestSetupOperator, jarDirectory, topology);
        }



        /**Task Runner Setup */
        taskRunnerExternal = new TaskRunnerExternal(taskRequestSetupOperator, null);
        //add the docker host to this task runner since we use containers
        taskRunnerExternal.setDockerHostID(dockerHostID);


        /**Create the docker file**/
        DockerFileCreator dockerFileCreator = new DockerFileCreator();
        DockerFile dockerfile = dockerFileCreator.createDockerFile(taskRequestSetupOperator.getDockerFileType(),jarDirectory);
        taskRunnerExternal.addDockerFile(dockerfile);


        /**All done. Now add the taskRunner to the taskDriver so we can track this taskRunner and manage it later*/
        taskDriver.addTaskRunner(taskRunnerExternal);


        /** Start the Container with the jar* */
        implementTaskRequest(
                "tcp://" + operatorInputBrokerIP +":61616"
                , "tcp://" + operatorOutputBrokerIP +":61616"
                , operatorInputFQQN
                , operatorOutputAddress
                , jarDirectory
                , "tcp://" + operatorInputBrokerIP +":61616"
                , topology.getMetricsTopologyAddress()
                , topology
                , taskRequestSetupOperator
                ,taskRunnerExternal
        );

    }



    private void implementTaskRequest(
            String operatorInputBrokerIP
            , String operatorOutputBrokerIP
            , String operatorInputFQQN
            , String operatorOutputAddress
            , String jarDirectory
            , String metrics_broker_IP_address
            , String metrics_FQQN
            , Topology topology
            , TaskRequestSetupOperator taskRequestSetupOperator
            , TaskRunnerExternal taskRunnerExternal) {


        String taskManagerID = taskDriver.getTaskBrokerConfig().getTaskManagerID();

        RoutingKeyConfig routingKeyConfig = new RoutingKeyConfig(taskRequestSetupOperator);

        System.out.println("[TaskRequestProcessorContainer]: process started: PredecessorSuccessorConfig");
        PredecessorSuccessorConfig predecessorSuccessorConfig = new PredecessorSuccessorConfig(
                "8000"
                ,null
                ,"NONE"
                , null
                ,taskRequestSetupOperator
                ,taskDriver.getDockerHostManager());

        System.out.println("[TaskWarmerConfig]: process started: TaskWarmerConfig");
        TaskWarmerConfig taskWarmerConfig = new TaskWarmerConfig(
                taskRequestSetupOperator
                ,topology
                ,predecessorSuccessorConfig
                ,routingKeyConfig.getRoutingKey());

        System.out.println("[DockerConfig]: process started: DockerConfig");
        DockerConfig dockerConfig = new DockerConfig(
                taskRunnerExternal
                ,topology
                ,taskRequestSetupOperator
                ,taskManagerID
                ,predecessorSuccessorConfig
                ,taskDriver.getDockerHostManager()
                );

        dockerConfig.instantiateOperatorContainer(
                jarDirectory
                ,operatorInputBrokerIP
                ,operatorInputFQQN
                ,operatorOutputBrokerIP
                ,operatorOutputAddress
                ,metrics_broker_IP_address
                ,metrics_FQQN
                ,taskRequestSetupOperator.getReconfigurationPlanID()
                ,taskWarmerConfig
                ,routingKeyConfig); //this launches the docker container


        /**********************************************************************************************************
         *  At this point in the setup, the containers are online but tuples are not yet able to flow to the
         *  containers. The activation logic will start the tuple flow to the containers.
         *
         *                  Topology and address activation logic
         *                               |
         *                              \/
         * ********************************************************************************************************/


        TopologyTransformer topologyTransformer = new TopologyTransformer(
                 topology
                ,taskRequestSetupOperator
                ,routingKeyConfig
                ,operatorInputFQQN
                ,taskDriver);

        topologyTransformer.transform(); // this picks a strategy and activates the tuple flow.


    }


    /**
     * Return the file name of the jar, NOT the path with the JAR
     * @param taskRequestMessage
     * @param taskRequestSetupOperator
     * @param jarPath
     * @return
     * @throws IOException
     */
    private String createTopologyJarFromActiveMQMessage(ClientMessage taskRequestMessage, TaskRequestSetupOperator taskRequestSetupOperator, String jarPath, Topology topology) throws IOException {

        String fileName = taskRequestSetupOperator.getTopologyID() + ".jar";
        String jarPathAndFilename = jarPath+fileName;
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try {
            taskRequestMessage.saveToOutputStream(bos);

        } catch (ActiveMQException e) {
            e.printStackTrace();
        }
        FileUtils.writeByteArrayToFile(new File(jarPathAndFilename), bos.toByteArray());
        bos.flush();
        bos.close();

        topology.setTopologyJarFileName(fileName);
        topology.setTopologyJarToBuilt(true);

        return fileName;
    }
}
