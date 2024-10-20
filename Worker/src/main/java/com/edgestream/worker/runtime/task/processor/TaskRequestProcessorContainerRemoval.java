package com.edgestream.worker.runtime.task.processor;

import com.edgestream.worker.io.MessageProducerClient;
import com.edgestream.worker.operator.Operator;
import com.edgestream.worker.operator.OperatorExternal;
import com.edgestream.worker.operator.OperatorID;
import com.edgestream.worker.runtime.task.TaskDriver;
import com.edgestream.worker.runtime.task.TaskRunner;
import com.edgestream.worker.runtime.task.model.TaskRequestSetupOperator;
import com.edgestream.worker.runtime.docker.DockerFileType;
import com.edgestream.worker.runtime.docker.DockerHost;
import com.edgestream.worker.runtime.reconfiguration.common.DagPlan;
import com.edgestream.worker.runtime.task.exception.EdgeStreamTaskRequestException;
import com.edgestream.worker.runtime.topology.Topology;
import com.edgestream.worker.runtime.topology.TopologyOperatorShutdownCoordinator;
import com.github.dockerjava.api.DockerClient;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.commons.lang3.SerializationUtils;

import java.util.ArrayList;


/***
 *
 * This class is responsible for handling operator shutdown requests.
 * 1. The request is parsed
 * 2. The relevant objects that manage the topolgies, the communication with the broker and the details of the request are then passed to the
 * {@link TopologyOperatorShutdownCoordinator} *
 *
 */

public class TaskRequestProcessorContainerRemoval {

    public TaskRequestProcessorContainerRemoval(ClientMessage taskRequestMessage, TaskDriver taskDriver) {

        /**************************************************************************************************************
         *
         *                             Task Request Configs Extraction
         *
         *
         * ***********************************************************************************************************/

        TaskRequestSetupOperator taskRequestSetupOperator = new TaskRequestSetupOperator();
        taskRequestSetupOperator.setTopologyID(taskRequestMessage.getStringProperty("Topology"));
        String reconfigurationPlanID = taskRequestMessage.getStringProperty("reconfigurationPlanID");

        OperatorID operatorID = new OperatorID(taskRequestMessage.getStringProperty("OperatorID"));
        String inputType = taskRequestMessage.getStringProperty("inputType"); // used to setup in the input queue filter
        String jarParams = taskRequestMessage.getStringProperty("OperatorType"); // used to setup the JAR file parameters

        ArrayList<String> operatorPredecessors = SerializationUtils.deserialize((byte[]) taskRequestMessage.getObjectProperty("OperatorPredecessorType"));
        ArrayList<String> operatorSuccessors = SerializationUtils.deserialize((byte[]) taskRequestMessage.getObjectProperty("OperatorSuccessorType"));

        String colocatedOperators = taskRequestMessage.getStringProperty("colocatedOperators");
        DagPlan dagPlan = SerializationUtils.deserialize((byte[]) taskRequestMessage.getObjectProperty("dagPlan"));
        taskRequestSetupOperator.setDagPlan(dagPlan);


        MessageProducerClient messageProducerClient = null;
        OperatorExternal operatorExternal = new OperatorExternal(messageProducerClient, operatorID, inputType);


        taskRequestSetupOperator.setOperator(operatorExternal);
        taskRequestSetupOperator.setOperatorID(taskRequestSetupOperator.getOperator().getOperatorID().getOperatorID_as_String()); //this value is preset by the cluster manager so we can get them from the Operator object
        taskRequestSetupOperator.setOperatorType(taskRequestSetupOperator.getOperator().getInputType()); //this value is preset by the cluster manager so we can get them from the Operator object
        taskRequestSetupOperator.setOperatorClassName(taskRequestSetupOperator.getOperator().getClass().getCanonicalName());


        taskRequestSetupOperator.setTask_id(taskRequestSetupOperator.getOperator().getOperatorID().getOperatorID_as_String()); //TODO: need to have cluster manager generate a unique task ID
        taskRequestSetupOperator.setTaskRunnerID(taskRequestSetupOperator.getOperator().getOperatorID().getOperatorID_as_String()); //TODO: if we get a unique task ID then we can generate a task runner ID
        taskRequestSetupOperator.setBroker_ip(taskDriver.getTaskBrokerConfig().getBroker_ip()); //The IP is set by the task manager so we use that, this may change in the future
        taskRequestSetupOperator.setDockerFileType(DockerFileType.valueOf(taskRequestMessage.getStringProperty("dockerFileType")));

        taskRequestSetupOperator.setDag(SerializationUtils.deserialize((byte[]) taskRequestMessage.getObjectProperty("dag")));


        System.out.println(taskRequestSetupOperator);


        /**************************************************************************************************************
         *
         *                             Remove operator from topology
         *
         *
         * ***********************************************************************************************************/


        //Step 1.check to see if topology already exists if so get it
        boolean existingTopology = taskDriver.topologyAlreadyExists(taskRequestSetupOperator.getTopologyID());

        Topology topology;

        if(existingTopology){

            topology = taskDriver.getTopology(taskRequestSetupOperator.getTopologyID());  //topology already exists, search for it

        }else{

            throw new EdgeStreamTaskRequestException("Could not find a topology with ID: " +  taskRequestSetupOperator.getTopologyID(), new Throwable());
        }


        TopologyOperatorShutdownCoordinator topologyOperatorShutdownCoordinator =
                new TopologyOperatorShutdownCoordinator(topology,taskRequestSetupOperator.getOperatorType(),taskDriver);

        topologyOperatorShutdownCoordinator.shutdownOperator();

    }
}
