package com.edgestream.worker.metrics.metricscollector2.state;

import com.edgestream.worker.metrics.metricscollector2.utils.GenericCollector;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSession;

import java.util.concurrent.ConcurrentLinkedDeque;

public class StateCollector extends GenericCollector {
    private ConcurrentLinkedDeque<StateMeasure> states = null;

    public StateCollector(ClientSession connector, ClientProducer activeMQProducer, String topology_id, String node_id, String operator_id, String buffer_consumer_size, String buffer_producer_size, String batch_size) {
        super(connector, activeMQProducer, topology_id, node_id, operator_id, buffer_consumer_size, buffer_producer_size, batch_size);
    }

    @Override
    public boolean isReady() {
        if (this.getStates() != null) {
            return (this.getStates().size() > 0);
        }
        return false;
    }

    @Override
    public boolean publish(boolean force) throws Exception {
        boolean commits = false;
        while (true) {
            if (this.getStates().peek() != null) {
                this.dumpMetrics(this.getStates().poll());
                commits = true;
            } else {
                break;
            }
        }
        return commits;
    }

    private boolean dumpMetrics(StateMeasure state) {
        System.out.println("\t\tSequence Id: " + getSequenceId().get() + "\n");

        //Create tuple
        ClientMessage msg = this.getConnector().createMessage(false);
        msg.putStringProperty("timeStamp", String.valueOf(state.getTimestamp()));
        msg.putStringProperty("sequence_id", String.valueOf(getSequenceId().get()));
        msg.putStringProperty("message_type", "application_state");
        msg.putStringProperty("topology_id", this.getTopologyId());
        msg.putStringProperty("node_id", this.getNodeId());
        msg.putStringProperty("operator_id", this.getOperatorId());
        //        msg.putStringProperty("batch_size", this.getBatchSize());
        msg.putStringProperty("buffer_consumer_size", this.getBufferConsumerSize());
        msg.putStringProperty("buffer_producer_size", this.getBufferProducerSize());
        msg.putStringProperty("tupleType", "metrics");
        msg.putStringProperty("state_type", String.valueOf(state.getStateType()));
        msg.putStringProperty("number_of_items", String.valueOf(state.getItems()));
        msg.putStringProperty("keys", String.valueOf(state.getKey()));
        msg.putStringProperty("bytes", String.valueOf(state.getSizeBytes()));
        msg.putStringProperty("window_time", String.valueOf(state.getWindowingTime()));

        try {
            this.getActiveMQProducer().send(msg);
        } catch (Exception e) {
            e.printStackTrace();
        }

        getSequenceId().getAndIncrement();
        return true;
    }

    public boolean add(StateMeasure.StateType stateType, String key, long sizeBytes, long items, long windowingTime) {
        if (this.getStates() == null) {
            this.setStates(new ConcurrentLinkedDeque<>());
        }

        this.getStates().add(new StateMeasure(stateType, key, sizeBytes, items, windowingTime));
        return true;
    }

    @Override
    public boolean close() throws Exception {
        if (this.isReady()) {
            return this.publish(true);
        }
        return false;
    }

    public ConcurrentLinkedDeque<StateMeasure> getStates() {
        return states;
    }

    public void setStates(ConcurrentLinkedDeque<StateMeasure> states) {
        this.states = states;
    }
}
