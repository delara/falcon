package com.edgestream.worker.metrics.metricscollector2.stream;

import com.edgestream.worker.metrics.metricscollector2.utils.GenericCollector;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSession;

import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.concurrent.ConcurrentLinkedDeque;

import static com.edgestream.worker.metrics.metricscollector2.utils.Utils.sd;
import static com.google.common.math.DoubleMath.mean;

public class StreamCollector extends GenericCollector {
    private ConcurrentLinkedDeque<StreamMeasure> incomingTuple = null;

    //Execution parameters
    private final int WINDOW_TIME = 1000;
    private final int MAX_IDLE_TIME = 20000;

    private Long idInput = 0L;

    public StreamCollector(ClientSession connector, ClientProducer activeMQProducer, String topology_id, String node_id, String operator_id, String buffer_consumer_size, String buffer_producer_size, String batch_size) {
        super(connector, activeMQProducer, topology_id, node_id, operator_id, buffer_consumer_size, buffer_producer_size, batch_size);
    }

    @Override
    public boolean isReady() {
        if (this.getIncomingTuple() != null) {
            return (this.getIncomingTuple().size() > 0) || (System.currentTimeMillis() - this.getLastArrival() > MAX_IDLE_TIME);
        }
        return false;
    }

    private void syncWindowTime() {
        if (this.getIncomingTuple() != null) {
            if (this.getIncomingTuple().size() > 0) {
                while (this.getIncomingTuple().getFirst().getTimestamp() > (this.getWindowStarting() + WINDOW_TIME)) {
                    this.setWindowStarting(this.getWindowStarting() + WINDOW_TIME);
                }
            }
        }
    }

    @Override
    public boolean publish(boolean force) throws Exception {
        boolean commits = false;
        int windowID = 0;
        this.syncWindowTime();
        while (true) {
            System.out.println("\n\nWINDOW " + windowID);
            System.out.println("\t Input");
            Long idInput = this.getMeasureIdWindow(this.getWindowStarting(), this.getIncomingTuple());

            if (idInput > -1) {
                ArrayList<StreamMeasure> input = null;
                if (idInput > -1) {
                    input = this.dequeue(idInput, this.getIncomingTuple());
                    System.out.println("Items to commit " + input.size());
                }

                if (input != null) {
                    this.dumpMetrics(input);
                    commits = true;
                }

                this.setWindowStarting(this.getWindowStarting() + WINDOW_TIME);
                windowID++;
            } else {
                break;
            }

        }

        if (force || (System.currentTimeMillis() - this.getLastArrival() > MAX_IDLE_TIME)) {
            ArrayList<StreamMeasure> input = this.dequeue(this.getIdInput() + 1, this.getIncomingTuple());

            if (input != null) {
                this.dumpMetrics(input);
                commits = true;
            }
        }

        return commits;
    }

    public boolean add(String previousOperatorId, String transferringTime, Long tupleSize) {
        if (this.getIncomingTuple() == null) {
            this.setIncomingTuple(new ConcurrentLinkedDeque<>());
        }

        if (this.getWindowStarting() == -1) {
            this.setWindowStarting(System.currentTimeMillis());
        }

        Duration duration = Duration.between(ZonedDateTime.parse(transferringTime), ZonedDateTime.now());
        Long transferTime = duration.toMillis();

//        System.out.println("Transfer time in millis:" + transferTime);

        long time = System.currentTimeMillis();
        this.getIncomingTuple().add(new StreamMeasure(this.getIdInput(), time, previousOperatorId, transferTime, tupleSize));
        this.setLastArrival(time);
        this.setIdInput(this.getIdInput() + 1);

        return true;
    }

    private Long getMeasureIdWindow(long time, ConcurrentLinkedDeque<StreamMeasure> queue) {
        Iterator<StreamMeasure> iterator = queue.iterator();
        Long id = Long.valueOf(-1);
        System.out.println("Size of queue: " + queue.size() + " Global queue size: " + this.getIncomingTuple().size() + " Time: " + time);
        while (iterator.hasNext()) {
            StreamMeasure measure = iterator.next();
            if (measure != null) {
                System.out.println("Measure id: " + measure.getId() + " has timestamp: " + measure.getTimestamp() + " at current ts: " + System.currentTimeMillis());
                if (measure.getTimestamp() < (time + WINDOW_TIME) && (time + WINDOW_TIME) < System.currentTimeMillis()) {
                    System.out.println("Id: " + measure.getId() +
                            " Time: " + measure.getTimestamp() +
                            " Window Start: " + time +
                            " Window close: " + (time + WINDOW_TIME));
                    id = measure.getId();
                } else {
                    break;
                }
            } else {
                break;
            }
        }
        System.out.println("Id returned from window: " + id);
        return id;
    }

    private ArrayList<StreamMeasure> dequeue(Long id, ConcurrentLinkedDeque<StreamMeasure> queue) {
        ArrayList<StreamMeasure> list = new ArrayList<>();
        while (true) {
            if (queue.peek() != null) {
                if (queue.peek().getId() <= id) {
                    list.add(queue.poll());
                } else {
                    break;
                }
            } else {
                break;
            }
        }

        if (list.size() > 0) {
            return list;
        } else {
            return null;
        }
    }

    private boolean dumpMetrics(ArrayList<StreamMeasure> input) {
        HashMap<String, ArrayList<Long>> edgeTransferringTime = new HashMap<>();
        HashMap<String, Long> edgeTransferringSize = new HashMap<>();
        ArrayList<String> keys = new ArrayList<>();

        for (int i = 0; i < input.size(); i++) {
            if (edgeTransferringTime.containsKey(input.get(i).getPreviousOperatorId())) {
                edgeTransferringTime.get(input.get(i).getPreviousOperatorId()).add(input.get(i).getTransferringTime());
                edgeTransferringSize.put(input.get(i).getPreviousOperatorId(), edgeTransferringSize.get(input.get(i).getPreviousOperatorId()) + input.get(i).getTupleSize());
            } else {
                ArrayList<Long> times = new ArrayList<>();
                times.add(input.get(i).getTransferringTime());
                edgeTransferringTime.put(input.get(i).getPreviousOperatorId(), times);
                edgeTransferringSize.put(input.get(i).getPreviousOperatorId(), input.get(i).getTupleSize());
                keys.add(input.get(i).getPreviousOperatorId());
            }
        }


        for (int i = 0; i < keys.size(); i++) {
            double max = Collections.max(edgeTransferringTime.get(keys.get(i))).doubleValue();
            double min = Collections.min(edgeTransferringTime.get(keys.get(i))).doubleValue();
//            print_stats_array(this.getEdgeTransferringTime().get(windowNumber).get(operators));

            Collections.sort(edgeTransferringTime.get(keys.get(i)));
            int size = edgeTransferringTime.get(keys.get(i)).size();
            int position = size / 2;
            double median = edgeTransferringTime.get(keys.get(i)).get(position).doubleValue();
            double meantransferringTime = mean(edgeTransferringTime.get(keys.get(i)));
            double std = sd(edgeTransferringTime.get(keys.get(i)), meantransferringTime);
            double transferringSize = edgeTransferringSize.get(keys.get(i)).doubleValue() / Double.valueOf(edgeTransferringTime.get(keys.get(i)).size());
            double edgeRate = edgeTransferringTime.get(keys.get(i)).size();


            System.out.println("\t\tEdge Sequence Id: " + getSequenceId().get() +
                    "\n\t\tSource Operator Id: " + keys.get(i) +
                    "\n\t\tMean Transferring Time (ms): " + meantransferringTime +
                    "\n\t\tMin Transferring Time (ms): " + min +
                    "\n\t\tMax Transferring Time (ms): " + max +
                    "\n\t\tMedian Transferring Time (ms): " + median +
                    "\n\t\tStd Transferring Time (ms): " + std +
                    "\n\t\tTransferring Size (bytes): " + transferringSize +
                    "\n\t\tRate: " + edgeRate + "\n");

            //Create tuple
            ClientMessage msg = this.getConnector().createMessage(false);
            msg.putStringProperty("timeStamp", String.valueOf(System.currentTimeMillis()));
            msg.putStringProperty("sequence_id", String.valueOf(getSequenceId().get()));
            msg.putStringProperty("message_type", "application_edges");
            msg.putStringProperty("topology_id", this.getTopologyId());
            msg.putStringProperty("node_id", this.getNodeId());
            msg.putStringProperty("transferring_time", String.valueOf(meantransferringTime));

            msg.putStringProperty("min_transferring_time", String.valueOf(min));
            msg.putStringProperty("max_transferring_time", String.valueOf(max));
            msg.putStringProperty("median_transferring_time", String.valueOf(median));
            msg.putStringProperty("std_transferring_time", String.valueOf(std));

            msg.putStringProperty("transferring_size", String.valueOf(transferringSize));
            msg.putStringProperty("input_rate", String.valueOf(edgeRate));
            msg.putStringProperty("operator_id", this.getOperatorId());
            msg.putStringProperty("previous_operator_id", keys.get(i));
            msg.putStringProperty("batch_size", this.getBatchSize());
            msg.putStringProperty("buffer_consumer_size", this.getBufferConsumerSize());
            msg.putStringProperty("buffer_producer_size", this.getBufferProducerSize());
            msg.putStringProperty("tupleType", "metrics");

            try {
                this.getActiveMQProducer().send(msg);
            } catch (Exception e) {
                e.printStackTrace();
            }

            getSequenceId().getAndIncrement();
        }


        return true;
    }

    @Override
    public boolean close() throws Exception {
        if (this.isReady()) {
            return this.publish(true);
        }
        return false;
    }

    public ConcurrentLinkedDeque<StreamMeasure> getIncomingTuple() {
        return incomingTuple;
    }

    public void setIncomingTuple(ConcurrentLinkedDeque<StreamMeasure> incomingTuple) {
        this.incomingTuple = incomingTuple;
    }

    public Long getIdInput() {
        return idInput;
    }

    public void setIdInput(Long idInput) {
        this.idInput = idInput;
    }
}
