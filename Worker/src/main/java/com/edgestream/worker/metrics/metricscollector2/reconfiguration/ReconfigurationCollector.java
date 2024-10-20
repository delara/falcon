package com.edgestream.worker.metrics.metricscollector2.reconfiguration;

import com.edgestream.worker.metrics.metricscollector2.utils.GenericCollector;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSession;

import java.util.concurrent.ConcurrentLinkedDeque;

public class ReconfigurationCollector extends GenericCollector {
    private ConcurrentLinkedDeque<ReconfigurationMeasure> reconf = null;

    public ReconfigurationCollector(ClientSession connector, ClientProducer activeMQProducer, String topology_id, String node_id, String operator_id, String buffer_consumer_size, String buffer_producer_size, String batch_size) {
        super(connector, activeMQProducer, topology_id, node_id, operator_id, buffer_consumer_size, buffer_producer_size, batch_size);
    }


    @Override
    public boolean isReady() {
        if (this.getReconf() != null) {
            return (this.getReconf().size() > 0);
        }
        return false;
    }

    @Override
    public boolean publish(boolean force) throws Exception {
        boolean commits = false;
        while (true) {
            if (this.getReconf().peek() != null) {
                this.dumpMetrics(this.getReconf().poll());
                commits = true;
            } else {
                break;
            }
        }
        return commits;
    }

    private boolean dumpMetrics(ReconfigurationMeasure reconfig) {
        System.out.println("\t\tSequence Id: " + getSequenceId().get() + "\n");
        //Create tuple
        ClientMessage msg = this.getConnector().createMessage(false);
        msg.putStringProperty("timeStamp", String.valueOf(reconfig.getTimestamp()));
        msg.putStringProperty("sequence_id", String.valueOf(getSequenceId().get()));
        msg.putStringProperty("message_type", "reconfiguration");
        msg.putStringProperty("topology_id", this.getTopologyId());
        msg.putStringProperty("node_id", this.getNodeId());
        msg.putStringProperty("operator_id", this.getOperatorId());
        msg.putStringProperty("tupleType", "metrics");
        msg.putStringProperty("operation_type", String.valueOf(reconfig.getOperation()));
        msg.putStringProperty("creation_timestamp", String.valueOf(reconfig.getCreationTimestamp()));

        try {
            this.getActiveMQProducer().send(msg);
        } catch (Exception e) {
            e.printStackTrace();
        }

        getSequenceId().getAndIncrement();
        return true;
    }

    public boolean add(ReconfigurationMeasure.ReconfigOperation operation, long creationTimestamp) {
        if (this.getReconf() == null) {
            this.setReconf(new ConcurrentLinkedDeque<>());
        }

        this.getReconf().add(new ReconfigurationMeasure( operation, creationTimestamp));
        return true;
    }

    @Override
    public boolean close() throws Exception {
        if (this.isReady()) {
            return this.publish(true);
        }
        return false;

    }

    public ConcurrentLinkedDeque<ReconfigurationMeasure> getReconf() {
        return reconf;
    }

    public void setReconf(ConcurrentLinkedDeque<ReconfigurationMeasure> reconf) {
        this.reconf = reconf;
    }

}
