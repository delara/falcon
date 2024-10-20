package com.edgestream.worker.metrics;

import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.client.*;
import org.apache.commons.lang3.SerializationUtils;

import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class MetricsCollector {
    static AtomicLong input_events = new AtomicLong(0);
    static AtomicLong output_events = new AtomicLong(0);
    static AtomicLong input_event_size = new AtomicLong(0);
    static AtomicLong output_event_size = new AtomicLong(0);
    static AtomicLong processing_time = new AtomicLong(0);
    static AtomicLong arrival_rate = new AtomicLong(0);
    static AtomicLong sending_rate = new AtomicLong(0);
    static AtomicLong end_to_end_latency = new AtomicLong(0);
    static AtomicReference timezoned_last_sending = new AtomicReference("");
    static AtomicReference timezoned_last_arrival = new AtomicReference("");
    static AtomicReference timezoned_msg_creation = new AtomicReference("");
    static AtomicReference timezoned_window_creation = new AtomicReference("");
    static AtomicBoolean new_window = new AtomicBoolean(true);
    private ArrayList<EdgeMetricsCollector> edgeMetrics = new ArrayList<EdgeMetricsCollector>();
    private ClientProducer activeMQProducer;
    private ClientSession connector;
    private ArrayList<String> originList = new ArrayList<>();

    private String topologyId;
    private String nodeId;
    private String operatorId;
    private int sequenceId = 0;
    private int windowTime = 1000;
    private long commitNumber;
    private String bufferConsumerSize;
    private String bufferProducerSize;
    private String batchSize;

    private EdgeMetricsCollector getEdgeMetric(String topologyId, String nodeId, String operatorId, String previousOperatorId, String currentOperatorId) throws Exception {
        if (this.getEdgeMetrics().size() > 0) {
            for (int i = 0; i < this.getEdgeMetrics().size(); i++) {
                if (this.getEdgeMetrics().get(i).getTopologyId().equals(topologyId) &&
                        this.getEdgeMetrics().get(i).getNodeId().equals(nodeId) &&
                        this.getEdgeMetrics().get(i).getOperatorId().equals(operatorId) &&
                        this.getEdgeMetrics().get(i).getPreviousOperatorId().equals(previousOperatorId) &&
                        this.getEdgeMetrics().get(i).getOperatorId().equals(this.getOperatorId())) {
                    return this.getEdgeMetrics().get(i);
                }
            }
        }
        this.getEdgeMetrics().add(new EdgeMetricsCollector(this.getWindowTime(), topologyId, nodeId, operatorId, previousOperatorId, this.getConnector(), this.getActiveMQProducer(), this.getBufferConsumerSize(), getBufferProducerSize(), this.getBatchSize()));
        return this.getEdgeMetrics().get(this.getEdgeMetrics().size() - 1);
    }

    public int getWindowTime() {
        return windowTime;
    }

    public void setWindowTime(int windowTime) {
        this.windowTime = windowTime;
    }

    public int getSequenceId() {
        return sequenceId;
    }

    public void setSequenceId(int sequenceId) {
        this.sequenceId = sequenceId;
    }

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

    public ClientSession getConnector() {
        return connector;
    }

    public void setConnector(ClientSession connector) {
        this.connector = connector;
    }

    public ClientProducer getActiveMQProducer() throws Exception {
        return activeMQProducer;
    }

    public void setActiveMQProducer(ClientProducer activeMQProducer) {
        this.activeMQProducer = activeMQProducer;
    }

    public String getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(String batchSize) {
        this.batchSize = batchSize;
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

    public ArrayList<EdgeMetricsCollector> getEdgeMetrics() {
        return edgeMetrics;
    }

    public void setEdgeMetrics(ArrayList<EdgeMetricsCollector> edgeMetrics) {
        this.edgeMetrics = edgeMetrics;
    }

    public ArrayList<String> getOriginList() {
        return originList;
    }

    public void setOriginList(ArrayList<String> originList) {
        this.originList = originList;
    }

    public void setOriginId(String originId){
        boolean existing = false;
        for (String s : this.getOriginList()){
            if (s.equals(originId)){
                existing = true;
                break;
            }
        }

        if (!existing){
            this.getOriginList().add(originId);
        }
    }

    private ClientProducer createLocalProducer(String metrics_server, String fqqn) {
        try {
            ServerLocator locator = ActiveMQClient.createServerLocator(metrics_server);
            locator.setConfirmationWindowSize(3000000);
            ClientSessionFactory factory = locator.createSessionFactory();
            this.setConnector(factory.createTransactedSession());
            this.getConnector().setSendAcknowledgementHandler(new MySendAcknowledgementsHandlerX());

            return this.getConnector().createProducer(fqqn);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public MetricsCollector(String metrics_server, String fqqn, String topology_id, String node_id, String operator_id, String buffer_consumer_size, String buffer_producer_size, String batch_size) {
        input_events.set(0);
        output_events.set(0);
        input_event_size.set(0);
        output_event_size.set(0);
        timezoned_last_sending.set("");
        timezoned_last_arrival.set("");

        this.setTopologyId(topology_id);
        this.setNodeId(node_id);
        this.setOperatorId(operator_id);
        this.setBatchSize(batch_size);
        this.setBufferConsumerSize(buffer_consumer_size);
        this.setBufferProducerSize(buffer_producer_size);

        this.setActiveMQProducer(this.createLocalProducer(metrics_server, fqqn));
    }

    private void publishMetric(float input_rate, float output_rate, float input_msg_size, float output_msg_size, float processing_time, float overall_latency) {
                /*The metrics are organized in the following manner separated by ";" in the input string:
                1) Total time
                2) Topology ID
                3) Node Id
                4) Throughput (tps)
                5) General End-to-end latency
                6) Operator ID
                7) Input message rate (tps)
                8) Average input message size
                9) Average output message size
                10) Time spent for local processing
                 */

        System.out.println("Application Sequence Id: " + getSequenceId() +
                " End-to-End App Latency (ms): " + overall_latency +
                " Input Rate(Msg/s): " + input_rate +
                " Output Rate (Msgs/s): " + output_rate +
                " Input Msg Size (bytes): " + input_msg_size +
                " Output Msg Size (bytes): " + output_msg_size +
                " Oper. Time(ns): " + processing_time);

        ClientMessage msg = this.getConnector().createMessage(false);
        msg.putStringProperty("timeStamp", String.valueOf(System.currentTimeMillis()));
        msg.putStringProperty("sequence_id", String.valueOf(this.getSequenceId()));
        msg.putStringProperty("message_type", "application");
        msg.putStringProperty("topology_id", this.getTopologyId());
        msg.putStringProperty("node_id", this.getNodeId());
        msg.putStringProperty("throughput", String.valueOf(output_rate));
        msg.putStringProperty("latency", String.valueOf(overall_latency));
        msg.putStringProperty("operator_id", this.getOperatorId());
        msg.putStringProperty("input_rate", String.valueOf(input_rate));
        msg.putStringProperty("input_msg_size", String.valueOf(input_msg_size));
        msg.putStringProperty("output_msg_size", String.valueOf(output_msg_size));
        msg.putStringProperty("processing_time", String.valueOf(processing_time));
        msg.putStringProperty("tupleType", "metrics");
        msg.putStringProperty("batch_size", this.getBatchSize());
        msg.putStringProperty("buffer_consumer_size", this.getBufferConsumerSize());
        msg.putStringProperty("buffer_producer_size", this.getBufferProducerSize());

        byte[] originList = SerializationUtils.serialize(this.getOriginList());
        msg.putObjectProperty("tupleOriginID",originList);

        try {
            this.getActiveMQProducer().send(msg);
        } catch (Exception e) {
            e.printStackTrace();
        }
        try {

            this.setCommitNumber(this.getCommitNumber() + 1);
            if (this.getCommitNumber() == 60) {
                this.getConnector().commit();
                this.setCommitNumber(0);
            }
        } catch (ActiveMQException e) {
            e.printStackTrace();
        }

        this.setSequenceId(this.getSequenceId() + 1);
    }

    private void setTimezonedLastSending(String lastSending) {
        timezoned_last_sending.set(lastSending);
    }

    public void setTimezonedMsgCreation(String creation) {
        timezoned_msg_creation.set(creation);
    }

    private void setTimezonedLastArrival(String lastArrival) {
        timezoned_last_arrival.set(lastArrival);
    }

    public long getInputEventSize() {
        return input_event_size.get();
    }

    public long getInputEvents() {
        return input_events.get();
    }

    public long getOutputEvents() {
        return output_events.get();
    }

    public long getOutputEventSize() {
        return output_event_size.get();
    }

    public void AddInputEventSize(long event_size, String previousOperatorId, String transferringTime) throws Exception {
        Duration duration = Duration.between(ZonedDateTime.parse(transferringTime), ZonedDateTime.now());
        long transf = duration.toMillis();
        this.getEdgeMetric(this.getTopologyId(), this.getNodeId(), this.getOperatorId(), previousOperatorId, this.getOperatorId()).addTransferringTime(transf, event_size);

        if (new_window.get()) {
            this.ClearInputEvents();
            this.ClearInputEventSize();
            arrival_rate.set(0);

            timezoned_window_creation.set(ZonedDateTime.now().toString());
            new_window.set(false);
        }

        input_event_size.set(input_event_size.get() + event_size);
        this.IncrementInputEvents();
        if (timezoned_last_arrival.get() != "") {
            ZonedDateTime z = ZonedDateTime.parse((String) timezoned_last_arrival.get());
            this.setTimezonedLastArrival(ZonedDateTime.now().toString());
            duration = Duration.between(z, ZonedDateTime.parse((String) timezoned_last_arrival.get()));
            arrival_rate.set(arrival_rate.get() + duration.toNanos());
        } else {
            this.setTimezonedLastArrival(ZonedDateTime.now().toString());
        }

    }

    private void IncrementInputEvents() {
        input_events.set(input_events.get() + 1);
    }

    public void AddOutputEventSize(long event_size) {
        output_event_size.set(output_event_size.get() + event_size);
        this.IncrementOutputEvents();
        Duration duration = null;
        if (timezoned_last_sending.get() != "") {
            ZonedDateTime z = ZonedDateTime.parse((String) timezoned_last_sending.get());
            this.setTimezonedLastSending(ZonedDateTime.now().toString());
            duration = Duration.between(z, ZonedDateTime.parse((String) timezoned_last_sending.get()));
            sending_rate.set(sending_rate.get() + duration.toNanos());
        } else {
            this.setTimezonedLastSending(ZonedDateTime.now().toString());
        }

        duration = Duration.between(ZonedDateTime.parse((String) timezoned_last_arrival.get()), ZonedDateTime.parse((String) timezoned_last_sending.get()));
        processing_time.set(processing_time.get() + duration.toNanos());

        duration = Duration.between(ZonedDateTime.parse((String) timezoned_msg_creation.get()), ZonedDateTime.parse((String) timezoned_last_sending.get()));
        end_to_end_latency.set(end_to_end_latency.get() + duration.toMillis());

        String s = (String) timezoned_window_creation.get();
        duration = Duration.between(ZonedDateTime.parse(s), ZonedDateTime.now());
        long time = duration.toMillis();

        if (time >= this.getWindowTime() && !new_window.get()) {
            float input_rate = input_events.get();
            float ir = (float) arrival_rate.get() / input_rate;
            float output_rate = output_events.get();
            float sl = (float) sending_rate.get() / output_rate;
            float input_size = ((float) input_event_size.get() / input_rate);
            float output_size = ((float) output_event_size.get() / output_rate);
            float processing_time = (float) MetricsCollector.processing_time.get() / input_rate;
            float overall_latency = (float) end_to_end_latency.get() / output_rate;

            this.publishMetric(input_rate, output_rate, input_size, output_size, processing_time, overall_latency);
            new_window.set(true);
            this.ClearOutputEvents();
            this.ClearOutputEventSize();
            sending_rate.set(0);
            MetricsCollector.processing_time.set(0);
            end_to_end_latency.set(0);
        }
    }

    private long stringByteSize(String payload) {
        return 8 * ((((payload.length()) * 2) + 45) / 8);
    }

    public void AddInputStringSize(String payload, String previousOperatorId, String transferringTime) throws Exception {
        this.AddInputEventSize(this.stringByteSize(payload), previousOperatorId, transferringTime);
    }

    public void AddOutputStringSize(String payload) {
        this.AddOutputEventSize(this.stringByteSize(payload));
    }

    private void IncrementOutputEvents() {
        output_events.set(output_events.get() + 1);
    }

    private void ClearInputEvents() {
        input_events.set(0);
    }

    private void ClearOutputEvents() {
        output_events.set(0);
    }

    private void ClearOutputEventSize() {
        output_event_size.set(0);
    }

    private void ClearInputEventSize() {
        input_event_size.set(0);
    }

    public long getCommitNumber() {
        return commitNumber;
    }

    public void setCommitNumber(long commitNumber) {
        this.commitNumber = commitNumber;
    }
}

class MySendAcknowledgementsHandlerX implements SendAcknowledgementHandler {
    @Override
    public void sendAcknowledged(final Message message) {

    }
}





class EdgeMetricsCollector {
    static AtomicLong number_events = new AtomicLong(0);
    static AtomicLong transferring_time = new AtomicLong(0);
    static AtomicLong transferring_data = new AtomicLong(0);
    static AtomicReference timezoned_window_creation = new AtomicReference("");
    static AtomicBoolean new_window = new AtomicBoolean(true);
    private String previousOperatorId = null;
    private String topologyId;
    private String nodeId;
    private String operatorId;
    private int sequenceId = 0;
    private int windowTime = 1000;
    private ClientProducer activeMQProducer;
    private ClientSession connector;
    private String bufferConsumerSize;
    private String bufferProducerSize;
    private String batchSize;

    public EdgeMetricsCollector(int windowTime, String topologyId, String nodeId, String operatorId, String previousOperatorId, ClientSession connector, ClientProducer activeMQProducer, String buffer_consumer_size, String buffer_producer_size, String batch_size) {
        this.setWindowTime(windowTime);
        this.setTopologyId(topologyId);
        this.setNodeId(nodeId);
        this.setOperatorId(operatorId);
        this.setPreviousOperatorId(previousOperatorId);
        this.setActiveMQProducer(activeMQProducer);
        this.setConnector(connector);
        this.setBufferConsumerSize(buffer_consumer_size);
        this.setBufferProducerSize(buffer_producer_size);
        this.setBatchSize(batch_size);
    }

    public void addTransferringTime(long transferTime, long transferData) {
        //transferTime is in nanoseconds
        if (new_window.get()) {
            number_events.set(0);
            transferring_data.set(0);
            transferring_time.set(0);
            timezoned_window_creation.set(ZonedDateTime.now().toString());
            new_window.set(false);
        }

        transferring_time.set(transferring_time.get() + transferTime);
        transferring_data.set(transferring_data.get() + transferData);
        number_events.set(number_events.get() + 1);

        String s = (String) timezoned_window_creation.get();
        Duration duration = Duration.between(ZonedDateTime.parse(s), ZonedDateTime.now());
        long time = duration.toMillis();

        if (time >= this.getWindowTime()) {
            float transfer_time = (float) transferring_time.get() / (float) number_events.get();
            float transfer_data = (float) transferring_data.get() / (float) number_events.get();
            int number_rate = (int) number_events.get();
            this.publishEdgeMetrics(transfer_time, transfer_data, number_rate);
            new_window.set(true);
        }

    }

    public void publishEdgeMetrics(float transfer_time, float transfer_size, int intput_rate) {
        ClientMessage msg = this.getConnector().createMessage(false);

        System.out.println("Edge Sequence Id: " + this.getSequenceId() +
                " Transferring Time (ns): " + transfer_time +
                " Transferring Size (bytes): " + transfer_size);

        msg.putStringProperty("timeStamp", String.valueOf(System.currentTimeMillis()));
        msg.putStringProperty("sequence_id", String.valueOf(this.getSequenceId()));
        msg.putStringProperty("message_type", "application_edges");
        msg.putStringProperty("topology_id", this.getTopologyId());
        msg.putStringProperty("node_id", this.getNodeId());
        msg.putStringProperty("transferring_time", String.valueOf(transfer_time));
        msg.putStringProperty("transferring_size", String.valueOf(transfer_size));
        msg.putStringProperty("input_rate", String.valueOf(intput_rate));
        msg.putStringProperty("operator_id", this.getOperatorId());
        msg.putStringProperty("previous_operator_id", this.getPreviousOperatorId());
        msg.putStringProperty("batch_size", this.getBatchSize());
        msg.putStringProperty("buffer_consumer_size", this.getBufferConsumerSize());
        msg.putStringProperty("buffer_producer_size", this.getBufferProducerSize());
        msg.putStringProperty("tupleType", "metrics");

        try {
            this.getActiveMQProducer().send(msg);
        } catch (Exception e) {
            e.printStackTrace();
        }

        this.setSequenceId(this.getSequenceId() + 1);
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


    public String getPreviousOperatorId() {
        return previousOperatorId;
    }

    public void setPreviousOperatorId(String previousOperatorId) {
        this.previousOperatorId = previousOperatorId;
    }

    public int getWindowTime() {
        return windowTime;
    }

    public void setWindowTime(int windowTime) {
        this.windowTime = windowTime;
    }

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

    public int getSequenceId() {
        return sequenceId;
    }

    public void setSequenceId(int sequenceId) {
        this.sequenceId = sequenceId;
    }


    public String getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(String batchSize) {
        this.batchSize = batchSize;
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


}