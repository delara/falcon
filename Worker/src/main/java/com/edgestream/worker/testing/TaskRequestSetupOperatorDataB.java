package com.edgestream.worker.testing;

import com.edgestream.worker.runtime.task.model.TaskRequestSetupOperator;

public class TaskRequestSetupOperatorDataB extends TaskRequestSetupOperator {

    String topologyID = "test_tp_1";
    String producer_client_iD = "ActiveMQ_Producer2";
    String consumer_client_iD = "ActiveMQ_Consumer2";
    String operatorID = "TypeB_1";
    String operatorType = "B";
    String task_id = "Task_B_1";
    String taskRunnerID = "task_runner_B_1";
    String broker_ip = "192.168.0.179";




    public String getTopologyID() {
        return topologyID;
    }

    public void setTopologyID(String topologyID) {
        this.topologyID = topologyID;
    }

    public String getProducer_client_iD() {
        return producer_client_iD;
    }

    public void setProducer_client_iD(String producer_client_iD) {
        this.producer_client_iD = producer_client_iD;
    }

    public String getConsumer_client_iD() {
        return consumer_client_iD;
    }

    public void setConsumer_client_iD(String consumer_client_iD) {
        this.consumer_client_iD = consumer_client_iD;
    }

    public String getOperatorID() {
        return operatorID;
    }

    public void setOperatorID(String operatorID) {
        this.operatorID = operatorID;
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
        this.task_id = task_id;
    }

    public String getTaskRunnerID() {
        return taskRunnerID;
    }

    public void setTaskRunnerID(String taskRunnerID) {
        this.taskRunnerID = taskRunnerID;
    }

    public String getBroker_ip() {
        return broker_ip;
    }

    public void setBroker_ip(String broker_ip) {
        this.broker_ip = broker_ip;
    }


    public TaskRequestSetupOperatorDataB(){

    }
}
