package com.edgestream.client;

import com.edgestream.worker.runtime.task.model.TaskWarmerStrategy;

import java.util.HashMap;

public class EdgeStreamApplicationConfig {

    private final String operatorInputBrokerIP;
    private final String operatorInputQueue;
    private final String operatorOutputBrokerIP;
    private final String operatorOutputAddress;
    private final String topologyID;
    private final String taskManagerID;
    private final String metrics_broker_IP_address;
    private final String metrics_FQQN;

    private final String predecessorInputMethod;
    private final String predecessorIP;
    private final String predecessorPort;
    private String successorIP;
    private final String reconfigurationPlanID;
    private final String TMToOPManagementFQQN;
    private final String OPtoOPManagementFQQN;
    private final String localOperatorMetricsFQQN;
    private final TaskWarmerStrategy warmupFlag;
    private final String warmupContainerIP;
    private final String warmupContainerOperatorID;
    private final String notifyPredecessorWhenReady;
    private final String waitForSuccessorReadyMessage;


    private final int producerBatchSize;
    private final int consumerBatchSize;
    private final int producerBufferSize;
    private final int consumerBufferSize;

    HashMap<String, String> typeToPortMap; // ex <"F","8000">


    public EdgeStreamApplicationConfig(String operatorInputBrokerIP
            , String operatorInputQueue
            , String operatorOutputBrokerIP
            , String operatorOutputAddress
            , String topologyID
            , String taskManagerID
            , String metrics_broker_IP_address
            , String metrics_FQQN
            , int producerBatchSize, int consumerBatchSize, int producerBufferSize, int consumerBufferSize
            , String predecessorInputMethod, String predecessorIP, String predecessorPort
            , String successorIP, HashMap typeToPortMap
            , String reconfigurationPlanID
            , String TMToOPManagementFQQN
            , String OPtoOPManagementFQQN
            , String localOperatorMetricsFQQN
            , String warmupFlag
            , String warmupContainerIP
            ,String warmupContainerOperatorID
            ,String notifyPredecessorWhenReady
            ,String waitForSuccessorReadyMessage) {


        this.operatorInputBrokerIP = operatorInputBrokerIP;
        this.operatorInputQueue = operatorInputQueue;
        this.operatorOutputBrokerIP = operatorOutputBrokerIP;
        this.operatorOutputAddress = operatorOutputAddress;
        this.topologyID = topologyID;
        this.taskManagerID = taskManagerID;
        this.metrics_broker_IP_address = metrics_broker_IP_address;
        this.metrics_FQQN = metrics_FQQN;
        this.producerBatchSize = producerBatchSize;
        this.consumerBatchSize = consumerBatchSize;
        this.producerBufferSize = producerBufferSize;
        this.consumerBufferSize = consumerBufferSize;
        this.predecessorInputMethod = predecessorInputMethod;
        this.predecessorIP = predecessorIP;
        this.predecessorPort = predecessorPort;
        this.successorIP = successorIP;
        this.typeToPortMap = typeToPortMap;
        this.reconfigurationPlanID = reconfigurationPlanID;
        this.TMToOPManagementFQQN = TMToOPManagementFQQN;
        this.OPtoOPManagementFQQN = OPtoOPManagementFQQN;
        this.localOperatorMetricsFQQN = localOperatorMetricsFQQN;
        this.warmupFlag = TaskWarmerStrategy.valueOf(warmupFlag);
        this.warmupContainerIP = warmupContainerIP;
        this.warmupContainerOperatorID = warmupContainerOperatorID;
        this.notifyPredecessorWhenReady = notifyPredecessorWhenReady;
        this.waitForSuccessorReadyMessage = waitForSuccessorReadyMessage;


    }

    public String getWaitForSuccessorReadyMessage() {
        return waitForSuccessorReadyMessage;
    }

    public String getNotifyPredecessorWhenReady() {
        return notifyPredecessorWhenReady;
    }

    public String getWarmupContainerIP() {
        return warmupContainerIP;
    }

    public TaskWarmerStrategy getWarmupFlag() {
        return warmupFlag;
    }

    public String getWarmupContainerOperatorID() {
        return warmupContainerOperatorID;
    }

    public String getLocalOperatorMetricsFQQN() {
        return localOperatorMetricsFQQN;
    }

    public String getTMToOPManagementFQQN() {
        return TMToOPManagementFQQN;
    }

    public String getOPtoOPManagementFQQN() {
        return OPtoOPManagementFQQN;
    }

    public String getReconfigurationPlanID() {
        return reconfigurationPlanID;
    }

    public HashMap<String, String> getTypeToPortMap() {
        return typeToPortMap;
    }

    public String getPredecessorInputMethod() {
        return predecessorInputMethod;
    }

    public String getSuccessorIP() {
        return successorIP;
    }

    public String getPredecessorIP() {
        return predecessorIP;
    }

    public String getPredecessorPort() {
        return predecessorPort;
    }


    public String getOperatorInputBrokerIP() {
        return operatorInputBrokerIP;
    }

    public String getOperatorInputQueue() {
        return operatorInputQueue;
    }

    public String getOperatorOutputBrokerIP() {
        return operatorOutputBrokerIP;
    }

    public String getOperatorOutputAddress() {
        return operatorOutputAddress;
    }

    public String getTopologyID() {
        return topologyID;
    }

    public String getTaskManagerID() {
        return taskManagerID;
    }

    public String getMetrics_broker_IP_address() {
        return metrics_broker_IP_address;
    }

    public String getMetrics_FQQN() {
        return metrics_FQQN;
    }
    public int getProducerBatchSize() {
        return producerBatchSize;
    }

    public int getConsumerBatchSize() {
        return consumerBatchSize;
    }

    public int getProducerBufferSize() {
        return producerBufferSize;
    }

    public int getConsumerBufferSize() {
        return consumerBufferSize;
    }


    public void setSuccessorIP(String successorIP) {
        this.successorIP = successorIP;
    }


}
