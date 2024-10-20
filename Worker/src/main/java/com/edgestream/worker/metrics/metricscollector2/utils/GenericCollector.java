package com.edgestream.worker.metrics.metricscollector2.utils;

import com.edgestream.worker.metrics.metricscollector2.operator.OperatorCollector;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSession;

import java.util.concurrent.atomic.AtomicLong;

public abstract class GenericCollector {
    private String topologyId;
    private String nodeId;
    private String operatorId;
    private String bufferConsumerSize;
    private String bufferProducerSize;
    private String batchSize;
    private ClientProducer activeMQProducer;
    private ClientSession connector;

    static AtomicLong sequenceId = new AtomicLong(0);
    private long lastArrival = -1;
    private long windowStarting = -1;


    public GenericCollector(ClientSession connector, ClientProducer activeMQProducer, String topology_id, String node_id, String operator_id, String buffer_consumer_size, String buffer_producer_size, String batch_size) {
        this.setTopologyId(topology_id);
        this.setNodeId(node_id);
        this.setOperatorId(operator_id);
        this.setBatchSize(batch_size);
        this.setBufferConsumerSize(buffer_consumer_size);
        this.setBufferProducerSize(buffer_producer_size);
        this.setActiveMQProducer(activeMQProducer);
        this.setConnector(connector);
    }

    public abstract boolean isReady();

    public abstract boolean publish(boolean force) throws Exception;

    public abstract boolean close() throws Exception;

    public String getTopologyId() {
        return topologyId;
    }

    public void setTopologyId(String topologyId) {
        this.topologyId = topologyId;
    }

    public String getNodeId() {
        return nodeId;
    }

    public void setNodeId(String nodeId) {
        this.nodeId = nodeId;
    }

    public String getOperatorId() {
        return operatorId;
    }

    public void setOperatorId(String operatorId) {
        this.operatorId = operatorId;
    }

    public String getBufferConsumerSize() {
        return bufferConsumerSize;
    }

    public void setBufferConsumerSize(String bufferConsumerSize) {
        this.bufferConsumerSize = bufferConsumerSize;
    }

    public String getBufferProducerSize() {
        return bufferProducerSize;
    }

    public void setBufferProducerSize(String bufferProducerSize) {
        this.bufferProducerSize = bufferProducerSize;
    }

    public String getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(String batchSize) {
        this.batchSize = batchSize;
    }

    public ClientProducer getActiveMQProducer() {
        return activeMQProducer;
    }

    public void setActiveMQProducer(ClientProducer activeMQProducer) {
        this.activeMQProducer = activeMQProducer;
    }

    public ClientSession getConnector() {
        return connector;
    }

    public void setConnector(ClientSession connector) {
        this.connector = connector;
    }


    public static AtomicLong getSequenceId() {
        return sequenceId;
    }

    public static void setSequenceId(AtomicLong sequenceId) {
        sequenceId = sequenceId;
    }

    public long getLastArrival() {
        return lastArrival;
    }

    public void setLastArrival(long lastArrival) {
        this.lastArrival = lastArrival;
    }

    public long getWindowStarting() {
        return windowStarting;
    }

    public void setWindowStarting(long windowStarting) {
        this.windowStarting = windowStarting;
    }
}
