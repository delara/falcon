package com.edgestream.worker.metrics.metricscollector2.statemanagement;

import com.edgestream.worker.metrics.metricscollector2.utils.GenericCollector;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSession;

import java.util.concurrent.ConcurrentLinkedDeque;

public class StateManagementCollector extends GenericCollector {
    private ConcurrentLinkedDeque<StateManagementMeasure> mngOperations = null;

    public StateManagementCollector(ClientSession connector, ClientProducer activeMQProducer, String topology_id, String node_id, String operator_id, String buffer_consumer_size, String buffer_producer_size, String batch_size) {
        super(connector, activeMQProducer, topology_id, node_id, operator_id, buffer_consumer_size, buffer_producer_size, batch_size);
    }

    @Override
    public boolean isReady() {
        if (this.getMngOperations() != null) {
            return this.getMngOperations().size() > 0;
        }
        return false;
    }

    @Override
    public boolean publish(boolean force) throws Exception {
        boolean commits = false;
        while (true) {
            if (this.getMngOperations().peek() != null) {
                this.dumpMetrics(this.getMngOperations().poll());
                commits = true;
            } else {
                break;
            }
        }
        return commits;
    }

    private boolean dumpMetrics(StateManagementMeasure mgnOperation) {
        System.out.println("\t\tSequence Id: " + getSequenceId().get() + "\n");

        //Create tuple
        ClientMessage msg = this.getConnector().createMessage(false);
        msg.putStringProperty("timeStamp", String.valueOf(mgnOperation.getTimestamp()));
        msg.putStringProperty("sequence_id", String.valueOf(getSequenceId().get()));
        msg.putStringProperty("message_type", "state_management");
        msg.putStringProperty("topology_id", this.getTopologyId());
        msg.putStringProperty("node_id", this.getNodeId());
        msg.putStringProperty("operator_id", this.getOperatorId());
        msg.putStringProperty("tupleType", "metrics");
        msg.putStringProperty("operation_type", String.valueOf(mgnOperation.getOperationType()));
        msg.putStringProperty("duration_time", String.valueOf(mgnOperation.getDuration()));
        msg.putStringProperty("number_of_items", String.valueOf(mgnOperation.getItems()));
        msg.putStringProperty("keys", String.valueOf(mgnOperation.getKeys()));
        msg.putStringProperty("bytes", String.valueOf(mgnOperation.getSizeBytes()));

        try {
            this.getActiveMQProducer().send(msg);
        } catch (Exception e) {
            e.printStackTrace();
        }

        getSequenceId().getAndIncrement();
        return true;
    }

    public boolean add(StateManagementMeasure.OperationType operationType, String keys, long sizeBytes, long items, long duration) {
        if (this.getMngOperations() == null) {
            this.setMngOperations(new ConcurrentLinkedDeque<>());
        }

        this.getMngOperations().add(new StateManagementMeasure(operationType, keys, sizeBytes, items, duration));
        return true;
    }

    @Override
    public boolean close() throws Exception {
        if (this.isReady()) {
            return this.publish(true);
        }
        return false;
    }

    public ConcurrentLinkedDeque<StateManagementMeasure> getMngOperations() {
        return mngOperations;
    }

    public void setMngOperations(ConcurrentLinkedDeque<StateManagementMeasure> mngOperations) {
        this.mngOperations = mngOperations;
    }
}
