package com.edgestream.worker.runtime.task.model;

import com.edgestream.worker.operator.Operator;
import com.edgestream.worker.runtime.docker.DockerFileType;
import com.edgestream.worker.runtime.reconfiguration.common.DagElementType;
import com.edgestream.worker.runtime.reconfiguration.common.DagPlan;
import com.edgestream.worker.runtime.reconfiguration.execution.ExecutionType;
import com.edgestream.worker.runtime.reconfiguration.state.StateTransferPlan;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import java.lang.reflect.Field;
import java.util.ArrayList;


/**
 * This class is used to  capture all the details of an operator setup request coming from the cluster manager.
 * The values are parsed from the incoming ActiveMQ message and added to this object.
 */
public class TaskRequestSetupOperator {


    private String topologyID = null;
    private String producer_client_iD = null;
    private String consumer_client_iD = null;
    private String operatorID = null;
    private String operatorClassName = null;
    private String operatorType = null;
    private String task_id = null;
    private String taskRunnerID = null;
    private String broker_ip = null;
    private Operator operator;
    private DockerFileType dockerFileType = null;
    private ArrayList<ImmutablePair> dag =null;
    private DagPlan dagPlan = null;
    private DagElementType dagElementType = null;
    private boolean hasBootstrapState = false;
    private StateTransferPlan stateTransferPlan = null;
    private String colocatedOperators = null;
    private ExecutionType executionType = null;
    private TaskWarmerStrategy taskWarmerStrategy = TaskWarmerStrategy.NO_INITIAL_DEPLOYMENT;
    private ArrayList<String> operatorPredecessors = null;
    private ArrayList<String> operatorSuccessors = null;
    private String reconfigurationPlanID =null;
    private boolean notifyPredecessorWhenReady = false;
    private boolean waitForSuccessorReadyMessage = false;




    public TaskRequestSetupOperator() {

    }

    /**
     * This flag determines whether this operator will notify its predecessors directly that its done warming up
     * @return
     */
    public String isNotifyPredecessorWhenReady() {
        if(notifyPredecessorWhenReady){
            return "YES_NOTIFY";
        }else{
            return "DO_NOT_NOTIFY";
        }
    }

    /**
     * This flag determines whether this operator will wait for a message from its successor that it has been warmed up
     * @return
     */
    public String isWaitForSuccessorReadyMessage() {
        if(waitForSuccessorReadyMessage){
            return "YES_WAIT";
        }else{
            return "DO_NOT_WAIT";
        }
    }

    public void setNotifyPredecessorWhenReady(boolean notifyPredecessorWhenReady) {
        this.notifyPredecessorWhenReady = notifyPredecessorWhenReady;
    }

    public void setWaitForSuccessorReadyMessage(boolean waitForSuccessorReadyMessage){
        this.waitForSuccessorReadyMessage = waitForSuccessorReadyMessage;
    }

    public String getReconfigurationPlanID() {
        return reconfigurationPlanID;
    }

    public void setReconfigurationPlanID(String reconfigurationPlanID) {
        this.reconfigurationPlanID = reconfigurationPlanID;
    }

    public ExecutionType getExecutionType() {
        return executionType;
    }

    public void setExecutionType(ExecutionType executionType) {
        this.executionType = executionType;
    }

    public TaskWarmerStrategy getTaskWarmerStrategy() {
        return taskWarmerStrategy;
    }

    public void setTaskWarmerStrategy(TaskWarmerStrategy taskWarmerStrategy) {
        this.taskWarmerStrategy = taskWarmerStrategy;
    }

    /****************************************************************
     *
     *              Stateful operator configs
     *
     ****************************************************************/

    public StateTransferPlan getStateTransferPlan() {
        return stateTransferPlan;
    }

    public void setStateTransferPlan(StateTransferPlan stateTransferPlan) {
        this.stateTransferPlan = stateTransferPlan;
    }

    public boolean hasBootstrapState() {
        return hasBootstrapState;
    }

    public void setHasBootstrapState(boolean hasBootstrapState) {
        this.hasBootstrapState = hasBootstrapState;
    }

    public DagElementType getDagElementType() {
        return dagElementType;
    }

    public void setDagElementType(DagElementType dagElementType) {
        this.dagElementType = dagElementType;
    }

    /****************************************************************
     *
     *              Stateful operator configs  END
     *
     ****************************************************************/


    public DockerFileType getDockerFileType() {
        return dockerFileType;
    }

    public void setDockerFileType(DockerFileType dockerFileType) {
        this.dockerFileType = dockerFileType;
    }

    public Operator getOperator() {
        return operator;
    }

    public void setOperator(Operator operator) {
        this.operator = operator;
    }

    public String getOperatorClassName() {
        return operatorClassName;
    }

    public void setOperatorClassName(String operatorClassName) {

        this.operatorClassName = operatorClassName;
    }

    public String getTopologyID() {
        return topologyID;
    }

    public void setTopologyID(String topologyID) {
        this.topologyID = topologyID;
    }

    public String getProducer_client_iD() {
        return producer_client_iD;
    }

    private void setProducer_client_iD() {
        this.producer_client_iD = "ActiveMQ_Producer_" + getTopologyID() + "_" + getOperatorID();
    }

    public String getConsumer_client_iD() {
        return consumer_client_iD;
    }

    private void setConsumer_client_iD() {
        this.consumer_client_iD = "ActiveMQ_Consumer_" + getTopologyID() + "_" +getOperatorID();
    }

    public String getOperatorID() {
        return operatorID;
    }

    public void setOperatorID(String operatorID) {

        this.operatorID = operatorID;
        //1. Set the producer ID and consumer ID now that you have both the topology id and operator id
        this.setProducer_client_iD();
        this.setConsumer_client_iD();

    }

    public String getOperatorType() {
        return operatorType;
    }

    public void setOperatorType(String operatorType) {
        this.operatorType = operatorType;
    }

    public String getTask_id() {
        return task_id;
    }

    public void setTask_id(String task_id) {
        this.task_id = "Task_ID_"+ task_id;
    }

    public String getTaskRunnerID() {
        return taskRunnerID;
    }

    public void setTaskRunnerID(String taskRunnerID) {
        this.taskRunnerID = "TaskRunner_" + taskRunnerID;
    }

    public String getBroker_ip() {
        return broker_ip;
    }

    public void setBroker_ip(String broker_ip) {
        this.broker_ip = broker_ip;
    }

    /******DAG Utils**************/

    public DagPlan getDagPlan() {
        return dagPlan;
    }

    public void setDagPlan(DagPlan dagPlan) {
        this.dagPlan = dagPlan;
    }




    public String getLastOperatorInDag(){

        int size = this.dag.size();
        ImmutablePair<String,String> lastElementInDag = null;
        if(size > 1) {
            lastElementInDag = this.dag.get(size - 1);
        }else{
            lastElementInDag = this.dag.get(0);
        }

        return lastElementInDag.left;

    }

    public ArrayList<ImmutablePair> getDag() {
        return dag;
    }

    public void setDag(ArrayList<ImmutablePair> dag) {
        this.dag = dag;
    }


    public String getColocatedOperators() {
        return colocatedOperators;
    }

    public void setColocatedOperators(String colocatedOperators) {
        this.colocatedOperators = colocatedOperators;
    }


    public ArrayList<String> getOperatorPredecessors() {
        return operatorPredecessors;
    }

    public void setOperatorPredecessors(ArrayList<String> operatorPredecessors) {
        this.operatorPredecessors = operatorPredecessors;
    }

    public ArrayList<String> getOperatorSuccessors() {
        return operatorSuccessors;
    }

    public void setOperatorSuccessors(ArrayList<String> operatorSuccessors) {
        this.operatorSuccessors = operatorSuccessors;
    }


    public boolean hasSuccessorsInDagPlan(){

        boolean hasSuccessorInDagPlan = false;

        ArrayList<ImmutablePair> dagElements = dagPlan.getDagElements();
        ArrayList<String> operatorSuccessors = getOperatorSuccessors();

        for(ImmutablePair dagElement : dagElements) {
            for (String operatorSuccessor : operatorSuccessors) {
                if (dagElement.left.equals(operatorSuccessor)) {
                    hasSuccessorInDagPlan = true;
                }
            }
        }

        return hasSuccessorInDagPlan;
    }

    public String toString() {

        System.out.println("------------Task Request Details---------------");

        StringBuilder result = new StringBuilder();
        String newLine = System.getProperty("line.separator");

        result.append( this.getClass().getName() );
        result.append( " Object {" );
        result.append(newLine);

        //determine fields declared in this class only (no fields of superclass)
        Field[] fields = this.getClass().getDeclaredFields();

        //print field names paired with their values
        for ( Field field : fields  ) {
            result.append("  ");
            try {
                result.append( field.getName() );
                result.append(": ");
                //requires access to private field:
                result.append( field.get(this) );
            } catch ( IllegalAccessException ex ) {
                System.out.println(ex);
            }
            result.append(newLine);
        }
        result.append("}");

        return result.toString();
    }

}
