package com.edgestream.worker.metrics.metricscollector2.operator;

import com.edgestream.worker.metrics.metricscollector2.utils.GenericCollector;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.commons.lang.SerializationUtils;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.ConcurrentLinkedDeque;

import static com.edgestream.worker.metrics.metricscollector2.utils.Utils.sd;
import static com.google.common.math.DoubleMath.mean;

public class OperatorCollector extends GenericCollector {
    private ConcurrentLinkedDeque<OperatorMeasure> input = null;
    private ConcurrentLinkedDeque<OperatorMeasure> output = null;

    //Execution parameters
    private final int WINDOW_TIME = 1000;
    private final int MAX_IDLE_TIME = 20000;

    private Long idInput = 0L;
    private Long idOutput = 0L;

    public OperatorCollector(ClientSession connector, ClientProducer activeMQProducer, String topology_id, String node_id, String operator_id, String buffer_consumer_size, String buffer_producer_size, String batch_size) {
        super(connector, activeMQProducer, topology_id, node_id, operator_id, buffer_consumer_size, buffer_producer_size, batch_size);
    }

    private void syncWindowTime() {
        if (this.getInput() != null || this.getOutput() != null) {
            if (this.getInput().size() > 0 || this.getOutput().size() > 0) {
                while (this.getInput().getFirst().getTimestamp() > (this.getWindowStarting() + WINDOW_TIME) && this.getOutput().getFirst().getTimestamp() > (this.getWindowStarting() + WINDOW_TIME)) {
                    this.setWindowStarting(this.getWindowStarting() + WINDOW_TIME);
                }
            }
        }
    }

    /**
     * The data stream application is structured as a DAG where operators are vertices and the streams are edges.
     * The metrics of both edge and vertex are computed according to windows. Each window stays active for @see #WINDOWTIME.
     * Once the @see #WINDOWTIME is exceeded then the method for publishing the metrics is called.
     * The metrics are computed per second, then the time is converted accordingly.
     * This function computes each operator metrics and then commit them to the metric system following the @see #COMMITNUMBER
     * to avoid overheads.
     */
    @Override
    public boolean publish(boolean force) {
        boolean commits = false;
        int windowID = 0;

//        this.syncWindowTime();

        while (true) {
//            System.out.println("\n\nWINDOW " + windowID);
//            System.out.println("\t Input");
            Long idInput = this.getMeasureIdWindow(this.getWindowStarting(), this.getInput());
//            System.out.println("\t Output");
            Long idOutput = this.getMeasureIdWindow(this.getWindowStarting(), this.getOutput());

            if (idInput > -1 || idOutput > -1) {
                ArrayList<OperatorMeasure> input = null;
                if (idInput > -1) {
                    input = this.dequeue(idInput, this.getInput());
                }

                ArrayList<OperatorMeasure> output = null;
                if (idOutput > -1) {
                    output = this.dequeue(idOutput, this.getOutput());
                }

                if (input != null || output != null) {
                    this.dumpMetrics(input, output);
                    commits = true;
                }

                this.setWindowStarting(this.getWindowStarting() + WINDOW_TIME);
                windowID++;
            } else {
                break;
            }

        }

        if (force || (System.currentTimeMillis() - this.getLastArrival() > MAX_IDLE_TIME)) {
            ArrayList<OperatorMeasure> input = this.dequeue(this.getIdInput() + 1, this.getInput());
            ArrayList<OperatorMeasure> output = this.dequeue(this.getIdOutput() + 1, this.getOutput());

            if (input != null || output != null) {
                this.dumpMetrics(input, output);
                commits = true;
            }
        }
        return commits;
    }

    private ArrayList<OperatorMeasure> dequeue(Long id, ConcurrentLinkedDeque<OperatorMeasure> queue) {
        ArrayList<OperatorMeasure> list = new ArrayList<>();
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

    private boolean dumpMetrics(ArrayList<OperatorMeasure> input, ArrayList<OperatorMeasure> output) {
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

//        System.out.println("\tStatistics");
        double inputSize = 0f;
        long inputRate = 0L;
        if (input != null && input.size() > 0) {
            for (int i = 0; i < input.size(); i++) {
                inputSize += input.get(i).getSize();
            }
            inputRate = input.size();
            inputSize = inputSize / input.size();
        }


        ArrayList<Long> e2e = new ArrayList<>();
        ArrayList<Long> processingTime = new ArrayList<>();
        double outputSize = 0f;
        long outputRate = 0L;
        double max = 0L;
        double min = 0L;
        double median = 0L;
        double meanLatency = 0L;
        double std = 0L;
        double processingTimeLocal = 0L;
        int size = 0;
        if (output != null && output.size() > 0) {
            for (int i = 0; i < output.size(); i++) {
                ZonedDateTime zdt = ZonedDateTime.ofInstant(Instant.ofEpochMilli(output.get(i).getTimestamp()), ZoneId.systemDefault());
                Duration duration = Duration.between(output.get(i).getCreationTime(), zdt);
                e2e.add(duration.toMillis());
                processingTime.add(output.get(i).getProcessingTime());
                outputSize += output.get(i).getSize();
            }
            outputRate = output.size();
            outputSize = outputSize / outputRate;
            max = Collections.max(e2e).doubleValue();
            min = Collections.min(e2e).doubleValue();

            Collections.sort(e2e);
            size = e2e.size();
            int position = size / 2;
            median = e2e.get(position).doubleValue();
            meanLatency = mean(e2e);
            std = sd(e2e, meanLatency);
            processingTimeLocal = mean(processingTime);
        }
//        System.out.println("Queue Sizes - Input: " + this.getInput().size() + " Output: " + this.getOutput().size());

//        System.out.println("\t\tApplication Sequence Id: " + this.getSequenceId().get() +
//                "\n\t\tAvg End-to-End App Latency (ms): " + meanLatency +
//                "\n\t\tMin End-to-End App Latency (ms): " + min +
//                "\n\t\tMax End-to-End App Latency (ms): " + max +
//                "\n\t\tMedian End-to-End App Latency (ms): " + median +
//                "\n\t\tStd End-to-End App Latency (ms): " + std +
//                "\n\t\tEnd-to-End App Latency (regs): " + size +
//                "\n\t\tInput Rate(Msg/s): " + inputRate +
//                "\n\t\tOutput Rate (Msg/s): " + outputRate +
//                "\n\t\tInput Msg Size (bytes): " + inputSize +
//                "\n\t\tOutput Msg Size (bytes): " + outputSize +
//                "\n\t\tOperator Time(ns): " + processingTimeLocal);

        //Create tuple
        ClientMessage msg = this.getConnector().createMessage(false);
        msg.putStringProperty("timeStamp", String.valueOf(System.currentTimeMillis()));
        msg.putStringProperty("sequence_id", String.valueOf(getSequenceId().get()));
        msg.putStringProperty("message_type", "application");
        msg.putStringProperty("topology_id", this.getTopologyId());
        msg.putStringProperty("node_id", this.getNodeId());
        msg.putStringProperty("throughput", String.valueOf(outputRate));
        msg.putStringProperty("latency", String.valueOf(meanLatency));

        msg.putStringProperty("min_latency", String.valueOf(min));
        msg.putStringProperty("max_latency", String.valueOf(max));
        msg.putStringProperty("median_latency", String.valueOf(median));
        msg.putStringProperty("std_latency", String.valueOf(std));
        msg.putStringProperty("operator_id", this.getOperatorId());
        msg.putStringProperty("input_rate", String.valueOf(inputRate));
        msg.putStringProperty("input_msg_size", String.valueOf(inputSize));
        msg.putStringProperty("output_msg_size", String.valueOf(outputSize));
        msg.putStringProperty("processing_time", String.valueOf(processingTimeLocal));
        msg.putStringProperty("tupleType", "metrics");
        msg.putStringProperty("batch_size", this.getBatchSize());
        msg.putStringProperty("buffer_consumer_size", this.getBufferConsumerSize());
        msg.putStringProperty("buffer_producer_size", this.getBufferProducerSize());

        byte[] originList = SerializationUtils.serialize(new ArrayList<String>());
        msg.putObjectProperty("tupleOriginID", originList);
        try {
            this.getActiveMQProducer().send(msg);
        } catch (ActiveMQException e) {
            e.printStackTrace();
            return false;
        }
        getSequenceId().getAndIncrement();

        return true;
    }

    private Long getMeasureIdWindow(long time, ConcurrentLinkedDeque<OperatorMeasure> queue) {
        Long id = Long.valueOf(-1);
        if (queue != null) {
            Iterator<OperatorMeasure> iterator = queue.iterator();
            while (iterator.hasNext()) {
                OperatorMeasure measure = iterator.next();
                if (measure != null) {
                    if (measure.getTimestamp() < (time + WINDOW_TIME) && (time + WINDOW_TIME) < System.currentTimeMillis()) {
//                        System.out.println("Id: " + measure.getId() +
//                                " Time: " + measure.getTimestamp() +
//                                " Window Start: " + time +
//                                " Window close: " + (time + WINDOW_TIME));
                        id = measure.getId();
                    } else {
                        break;
                    }
                } else {
                    break;
                }
            }
        }

        return id;
    }

    @Override
    public boolean close() throws Exception {
        if (this.isReady()) {
            return this.publish(true);
        }
        return false;

    }

    /**
     * The data stream application is structured as a DAG where operators are vertices and the streams are edges.
     * This method stores information of a tuple (when it was created by the producer and when it arrived to this current operator),
     * and it also computes metrics for each edge of the DAG.
     *
     * @param event_size - this is the size of the tuple (bytes) when it arrived in the operator before the operator transformation
     */
    public void addInput(Long event_size) {
        if (this.getInput() == null) {
            this.setInput(new ConcurrentLinkedDeque<>());
        }

        if (this.getWindowStarting() == -1) {
            this.setWindowStarting(System.currentTimeMillis());
        }

        long time = System.currentTimeMillis();
        this.getInput().add(new OperatorMeasure(time, this.getIdInput(), event_size));
        this.setLastArrival(time);
        this.setIdInput(this.getIdInput() + 1);
    }

    /**
     * The data stream application is structured as a DAG where operators are vertices and the streams are edges.
     * This methods computes the values of processing time, output rate and output tuple size for a DAG vertex.
     *
     * @param event_size     - this is the output size (bytes) of the tuple after passing by the operator transformation.
     * @param processingTime - In Nanoseconds
     */

    public void addOutput(Long event_size, String timeStampFromSource, Long processingTime) {
        if (this.getOutput() == null) {
            this.setOutput(new ConcurrentLinkedDeque<>());
        }

        if (this.getWindowStarting() == -1) {
            this.setWindowStarting(System.currentTimeMillis());
        }

        long time = System.currentTimeMillis();
        this.getOutput().add(new OperatorMeasure(time, this.getIdOutput(), event_size, ZonedDateTime.parse(timeStampFromSource), processingTime));
        this.setLastArrival(time);
        this.setIdOutput(this.getIdOutput() + 1);
    }


    public boolean isReady() {
        if (this.getInput() != null && this.getOutput() != null) {
            return (this.getInput().size() > 0 || this.getOutput().size() > 0) || (System.currentTimeMillis() - this.getLastArrival() > MAX_IDLE_TIME);
        } else if (this.getInput() == null && this.getOutput() != null) {
            return (this.getOutput().size() > 0) || (System.currentTimeMillis() - this.getLastArrival() > MAX_IDLE_TIME);
        } else if (this.getInput() != null && this.getOutput() == null) {
            return (this.getInput().size() > 0) || (System.currentTimeMillis() - this.getLastArrival() > MAX_IDLE_TIME);
        }
        return false;

    }

    public ConcurrentLinkedDeque<OperatorMeasure> getInput() {
        return input;
    }

    public void setInput(ConcurrentLinkedDeque<OperatorMeasure> input) {
        this.input = input;
    }

    public ConcurrentLinkedDeque<OperatorMeasure> getOutput() {
        return output;
    }

    public void setOutput(ConcurrentLinkedDeque<OperatorMeasure> output) {
        this.output = output;
    }


    public Long getIdInput() {
        return idInput;
    }

    public Long getIdOutput() {
        return idOutput;
    }

    public void setIdInput(Long idInput) {
        this.idInput = idInput;
    }

    public void setIdOutput(Long idOutput) {
        this.idOutput = idOutput;
    }

}
