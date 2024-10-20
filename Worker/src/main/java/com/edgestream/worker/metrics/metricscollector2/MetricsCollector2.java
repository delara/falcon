package com.edgestream.worker.metrics.metricscollector2;

import com.edgestream.worker.metrics.common.StateCollection;
import com.edgestream.worker.metrics.common.StateValues;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.client.*;
import org.apache.commons.lang3.SerializationUtils;

import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.math.DoubleMath.mean;

/***
 * @author Alexandre da Silva Veith
 * The @see #MetricsCollector2 computes both operator and edge metrics. The data stream application is structured as
 * a DAG where operators are vertices and the streams are edges. Each operator assignment (individual or fused operators)
 * runs along with a system to collect operator/edge execution metrics. The metric system accumulates metrics in a window
 * for @see #WINDOWTIME, and then a new window is created. After the system creates a new window, the metrics stored in
 * the old window are used to compute average values. Then the resulting values are sent to a ActiveMQ broker.
 * To reduce the communication between the producer and broker, the system creates a buffer of @see #COMMITNUMBER metric tuples before
 * committing them to the broker. If something happens and the system becomes inactive, then the buffer is committed
 * without considering the target buffer (@see #COMMITNUMBER).
 */

public class MetricsCollector2 {
    final Object lock = new Object();
    Thread publish;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicBoolean closed = new AtomicBoolean(false);
    public AtomicBoolean deleteInternalID = new AtomicBoolean(true);

    //Metric system
    static AtomicLong sequenceId = new AtomicLong();
    HashMap<Integer, Long> inputRate = new HashMap<>();
    HashMap<Integer, Long> outputRate = new HashMap<>();
    HashMap<Integer, Long> inputSize = new HashMap<>();
    HashMap<Integer, Long> outputSize = new HashMap<>();
    HashMap<Integer, ArrayList<Long>> endToEndLatency = new HashMap<>();
    HashMap<Integer, Long> processingTime = new HashMap<>();

    //Edges metric system
    HashMap<Integer, ArrayList<String>> registeredOperators = new HashMap<>();
    HashMap<Integer, HashMap<String, Long>> edgeTransferringSize = new HashMap<>();
    HashMap<Integer, HashMap<String, ArrayList<Long>>> edgeTransferringTime = new HashMap<>();
    HashMap<Integer, HashMap<String, Long>> edgeTransferringRate = new HashMap<>();
    HashMap<String, AtomicLong> edgeSequenceID = new HashMap<>();

    //Operator consistency
    HashMap<Integer, ArrayList<String>> receivingControl = new HashMap<>();

    //Operator state
    HashMap<Integer, StateCollection> stateControl = new HashMap<>();
    private String keyType;

    //Control window metrics
    private HashMap<String, ZonedDateTime> firstEndToEndLatency = new HashMap<>();
    private HashMap<String, ZonedDateTime> firstTupleReceiving = new HashMap<>();

    //Support system
//    HashMap<String, ZonedDateTime> timestampMsgCreation = new HashMap<>(); //this was changed by firstEndToEndLatency
//    HashMap<String, ZonedDateTime> timestampMsgArrival = new HashMap<>(); //this was changed by firstTupleReceiving
    HashMap<Integer, ZonedDateTime> lastSending = new HashMap<>();
    HashMap<Integer, ZonedDateTime> firstReceiving = new HashMap<>();
    static AtomicLong commitCount = new AtomicLong(0);
    static AtomicReference<ZonedDateTime> lastTupleReceiving = new AtomicReference();


    //Metric management
    static AtomicInteger activeWindowNumber = new AtomicInteger(0); //The system has windows - the windows are used to control the metrics
    HashMap<Integer, ZonedDateTime> timestampWindowCreation = new HashMap<>();

    //Execution parameters
    final int WINDOW_TIME = 1000; //Metrics are accumulated for 1s.
    final int COMMIT_NUMBER = 10;
    final int INACTIVE_CYCLES = 10;
    final int AVAILABLE_WINDOWS = 30;
    final double SAFE_MARGIN = .15;
    final int IDLE_TIME = 10000;
    final int OUTPUT_SAMPLES = 5;

    //ActiveMQ connection
    private ClientProducer activeMQProducer;
    private ClientSession connector;

    //Information about the operator
    private String topologyId;
    private String nodeId;
    private String operatorId;
    private String bufferConsumerSize;
    private String bufferProducerSize;
    private String batchSize;
    private ArrayList<String> originList = new ArrayList<>(); //This is the computing resource name where the producer is located
    private boolean checkConsistency = false;
    private boolean checkState = false;

    /**
     * The constructor creates an ActiveMQ connection for publishing metric tuples using @param metrics_server, and @param fqqn.
     * Each metric tuple has a label to identify the @param topology_id, @param node_id, @param operator_id,
     * and @param operator_id. The label also contains details on how the ActiveMQ producer and consumer were configured
     * (@param buffer_consumer_size, @param buffer_producer_size, and @param batch_size).
     * A thread controls when closing and opening metric windows. There are @see #AVAILABLE_WINDOWS windows to be used in a RR fashion by the system.
     * The thread also has a trigger to send metrics when the system is not active (i.e., exceeds @see #INACTIVE_CYCLES cycles without any arrival).
     * This inactive period can happen due to operator migrations.
     * If the system is active than the metrics are committed according to the @see #WINDOWTIME.
     *
     * @param metrics_server       - this is the ip address and port to create the connection
     * @param fqqn                 - this is the artemis queue + address or only the queue to where the tuples will be sent
     * @param topology_id          - this is the application topology id
     * @param node_id              - this is the ID of the computing resource where the operator is running
     * @param operator_id          - this is the operator ID instantiated with the MetricsCollector
     * @param buffer_consumer_size - this is the buffer parameter used by the Artemis consumer
     * @param buffer_producer_size - this is the buffer parameter used by the Artemis producer
     * @param batch_size           - this is the batch size used to commit the operator tuples
     */
    public MetricsCollector2(String metrics_server, String fqqn, String topology_id, String node_id, String operator_id, String buffer_consumer_size, String buffer_producer_size, String batch_size) {

        //Setup the operator's info
        this.setTopologyId(topology_id);
        this.setNodeId(node_id);
        this.setOperatorId(operator_id);
        this.setBatchSize(batch_size);
        this.setBufferConsumerSize(buffer_consumer_size);
        this.setBufferProducerSize(buffer_producer_size);

        //Start publishing thread
        this.createControlThread(metrics_server, fqqn);


    }

//    /**
//     * The constructor creates an ActiveMQ connection for publishing metric tuples using @param metrics_server, and @param fqqn.
//     * Each metric tuple has a label to identify the @param topology_id, @param node_id, @param operator_id,
//     * and @param operator_id. The label also contains details on how the ActiveMQ producer and consumer were configured
//     * (@param buffer_consumer_size, @param buffer_producer_size, and @param batch_size).
//     * A thread controls when closing and opening metric windows. There are @see #AVAILABLE_WINDOWS windows to be used in a RR fashion by the system.
//     * The thread also has a trigger to send metrics when the system is not active (i.e., exceeds @see #INACTIVE_CYCLES cycles without any arrival).
//     * This inactive period can happen due to operator migrations.
//     * If the system is active than the metrics are committed according to the @see #WINDOWTIME.
//     *
//     * @param metrics_server       - this is the ip address and port to create the connection
//     * @param fqqn                 - this is the artemis queue + address or only the queue to where the tuples will be sent
//     * @param topology_id          - this is the application topology id
//     * @param node_id              - this is the ID of the computing resource where the operator is running
//     * @param operator_id          - this is the operator ID instantiated with the MetricsCollector
//     * @param buffer_consumer_size - this is the buffer parameter used by the Artemis consumer
//     * @param buffer_producer_size - this is the buffer parameter used by the Artemis producer
//     * @param batch_size           - this is the batch size used to commit the operator tuples
//     * @param key_value            - this is the value to keep track of the correctness when migrating a key
//     */
//    public MetricsCollector2(String metrics_server, String fqqn, String topology_id, String node_id, String operator_id, String buffer_consumer_size, String buffer_producer_size, String batch_size, String key_value) {
//
//        //Setup the operator's info
//        this.setTopologyId(topology_id);
//        this.setNodeId(node_id);
//        this.setOperatorId(operator_id);
//        this.setBatchSize(batch_size);
//        this.setBufferConsumerSize(buffer_consumer_size);
//        this.setBufferProducerSize(buffer_producer_size);
//
//        //Start publishing thread
//        this.createControlThread(metrics_server, fqqn);
//
//        //Key
//        this.setKeyValue(key_value);
//
//
//    }

    /**
     * A thread controls when closing and opening metric windows. There are @see #AVAILABLE_WINDOWS windows to be used in a RR fashion by the system.
     * The thread also has a trigger to send metrics when the system is not active (i.e., exceeds @see #INACTIVE_CYCLES cycles without any arrival - following a idle time limit @IDLE_TIME).
     * This inactive period can happen due to operator migrations.
     * If the system is active than the metrics are committed according to the @see #WINDOWTIME and @OUTPUT_SAMPLES (i.e., the number of processed tuples).
     *
     * @param metrics_server - this is the ip address and port to create the connection
     * @param fqqn           - this is the artemis queue + address or only the queue to where the tuples will be sent
     */
    private void createControlThread(String metrics_server, String fqqn) {
        //Initiate the management windows
        this.clearWindow(0);
        activeWindowNumber.set(0);

        //Create ActiveMQ Producer
        this.setActiveMQProducer(this.createLocalProducer(metrics_server, fqqn));

        //Start publishing thread
        this.setPublish(new Thread(() -> {
            running.set(true);
            closed.set(false);

            try {

                int countInactive = 0;
                while (running.get()) {
                    if ((this.getOutputRate().get(activeWindowNumber.get()) > 0) || (this.getInputRate().get(activeWindowNumber.get()) > 0)) {

                    /*
                    Computes the time since the window was created
                     */
                        ZonedDateTime windowOpening;
                        if (getTimestampWindowCreation().isEmpty()) {
                            windowOpening = ZonedDateTime.now();
                            getTimestampWindowCreation().put(activeWindowNumber.get(), windowOpening);
                        }
                        windowOpening = getTimestampWindowCreation().get(activeWindowNumber.get());

                        ZonedDateTime windowClosing = ZonedDateTime.now();
                        Duration duration = Duration.between(windowOpening, windowClosing);
                        long time = duration.toMillis();

                        //If the window exceeds the @see #WINDOWTIME than the method for publishing the metrics is called
                        if (time >= WINDOW_TIME) {

                            //The idle time is verified according to the time of the last tuple arrival in the operator
                            double idleTime = Duration.between(lastTupleReceiving.get(), ZonedDateTime.now()).toMillis();

                            //if the idle time exceeds a limit then the system is considered idle
                            if (idleTime <= IDLE_TIME) {

                                //the number of output tuples must be higher then @OUTPUT_SAMPLES to have enough samples to compute the metrics
                                if (getOutputRate().get(activeWindowNumber.get()) > OUTPUT_SAMPLES) {
                                    synchronized (lock) {
                                        //Gets the active window
                                        int windowNumber = activeWindowNumber.get();

                                        //Apply corrections to the window time
                                        //time = getAdjustedWindowTime(windowOpening, windowClosing, windowNumber);

                                        //Change the active window number using Round-Robin
                                        if (windowNumber < AVAILABLE_WINDOWS) {
                                            clearWindow(windowNumber + 1);
                                            getTimestampWindowCreation().put(windowNumber + 1, ZonedDateTime.now());
                                            activeWindowNumber.set(windowNumber + 1);
                                        } else {
                                            clearWindow(0);
                                            getTimestampWindowCreation().put(0, ZonedDateTime.now());
                                            activeWindowNumber.set(0);
                                        }

                                        //Publish metrics
                                        this.publishMetrics(windowNumber, time);

                                        countInactive = 0;


                                    }
                                }
                            } else {
                                //Counts the inactive cycles
                                countInactive++;

                                //Keeps using the same window
                                getTimestampWindowCreation().put(activeWindowNumber.get(), ZonedDateTime.now());
                            }
                        }

                        //When the system achieves 30 inactive cycles and also exists uncommitted metrics then the
                        // uncommitted metrics are sent to the metric system
                        if (countInactive == INACTIVE_CYCLES && commitCount.get() > 0) {
                            getConnector().commit();
                            commitCount.set(0);
                        }

                        Thread.sleep(10);
                    } else {
                        synchronized (this.getPublish()) {
                            this.getPublish().wait();
                        }
                    }
                }


            } catch (ActiveMQException | InterruptedException v) {
                v.printStackTrace();
            }

            closed.set(true);
        }));

        this.getPublish().setName("MetricsCollector");
        this.getPublish().start();
    }

    /**
     * This function creates a connection to the ActiveMQ broker.
     *
     * @param metrics_server - this is the ip address and port to create the connection
     * @param fqqn           - this is the ActiveMQ queue + address or only the queue to where the tuples will be sent.
     * @return - the producer connector
     */
    private ClientProducer createLocalProducer(String metrics_server, String fqqn) {
        try {
            ServerLocator locator = ActiveMQClient.createServerLocator(metrics_server);
            locator.setConfirmationWindowSize(3000000);
            locator.setBlockOnDurableSend(false);
            locator.setBlockOnNonDurableSend(false);
            locator.setBlockOnAcknowledge(false);

            ClientSessionFactory factory = locator.createSessionFactory();
            this.setConnector(factory.createTransactedSession());

            return this.getConnector().createProducer(fqqn);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * This function clears the values of a window from both operator and edge metrics.
     *
     * @param windowNumber - window number to be cleaned.
     */
    private void clearWindow(int windowNumber) {
        //Operator
        this.getInputRate().put(windowNumber, 0l);
        this.getOutputRate().put(windowNumber, 0l);
        this.getInputSize().put(windowNumber, 0l);
        this.getOutputSize().put(windowNumber, 0l);

        this.getEndToEndLatency().remove(windowNumber);
        this.getEndToEndLatency().put(windowNumber, new ArrayList<>());
        this.getEndToEndLatency().get(windowNumber).clear();

        this.getProcessingTime().put(windowNumber, 0l);
        this.getReceivingControl().remove(windowNumber);

        this.getReceivingControl().remove(windowNumber);
        this.getReceivingControl().put(windowNumber, new ArrayList<>());

        this.getStateControl().remove(windowNumber);
        this.getStateControl().put(windowNumber, new StateCollection());
        lastTupleReceiving.set(ZonedDateTime.now());

        //window
        this.getFirstReceiving().remove(windowNumber);
        this.getLastSending().remove(windowNumber);

        //Edges
        this.getRegisteredOperators().remove(windowNumber);
        this.getEdgeTransferringRate().remove(windowNumber);
        this.getEdgeTransferringSize().remove(windowNumber);

        this.getEdgeTransferringTime().remove(windowNumber);

    }

    /**
     * This function includes the id of the computing resource where the tuple was generated.
     *
     * @param originId - id of the computing resource
     */
    public void includeOriginId(String originId) {
        boolean existing = false;
        for (String s : this.getOriginList()) {
            if (s.equals(originId)) {
                existing = true;
                break;
            }
        }

        if (!existing) {
            this.getOriginList().add(originId);
        }
    }

    public static double sd(ArrayList<Long> table, double mean) {
        // Step 1:
//        double mean = mean(table)/1000000000;
        double temp = 0d;

        for (int i = 0; i < table.size(); i++) {
            double val = table.get(i).doubleValue() / 1000000;

            // Step 2:
            double squrDiffToMean = Math.pow(val - mean, 2);

            // Step 3:
            temp += squrDiffToMean;
        }

        // Step 4:
        double meanOfDiffs = temp / (double) (table.size());

        // Step 5:
        return Math.sqrt(meanOfDiffs);
    }


    public static void print_stats_array(ArrayList<Long> list) {
        System.out.println("======== Array ==========\n");
        for (int i = 0; i < list.size(); i++) {
            System.out.print(list.get(i) + "\n");
        }
    }

    /*
        Method to publish metrics to ActiveMQ
     */

    /**
     * The data stream application is structured as a DAG where operators are vertices and the streams are edges.
     * The metrics of both edge and vertex are computed according to windows. Each window stays active for @see #WINDOWTIME.
     * Once the @see #WINDOWTIME is exceeded then the method for publishing the metrics is called.
     * The metrics are computed per second, then the time is converted accordingly.
     * This function computes each operator metrics and then commit them to the metric system following the @see #COMMITNUMBER
     * to avoid overheads.
     *
     * @param windowNumber - this is the number of the window where the metrics were stored.
     * @param time         - this is the time in ms where the metrics were accumulated.
     */
    private void publishOperatorMetric(int windowNumber, long time) {
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

        //Compute the metrics to be sent
//        Long sum = this.getEndToEndLatency().get(windowNumber).stream()
//                .reduce(0l, Long::sum);

        System.out.println("Size of the array:" + this.getEndToEndLatency().get(windowNumber).size());
        double max = Collections.max(this.getEndToEndLatency().get(windowNumber)).doubleValue() / 1000000;
        double min = Collections.min(this.getEndToEndLatency().get(windowNumber)).doubleValue() / 1000000;
//        print_stats_array(this.getEndToEndLatency().get(windowNumber));

        Collections.sort(this.getEndToEndLatency().get(windowNumber));
        int size = this.getEndToEndLatency().get(windowNumber).size();
        int position = size / 2;
        double median = this.getEndToEndLatency().get(windowNumber).get(position).doubleValue() / 1000000;
        double meanLatency = mean(this.getEndToEndLatency().get(windowNumber)) / 1000000;

//        double latencyLocal = ((sum.doubleValue()/1000000000) / this.getOutputRate().get(windowNumber));
        double std = sd(this.getEndToEndLatency().get(windowNumber), meanLatency);
        double outputRateLocal = this.getOutputRate().get(windowNumber).doubleValue() / ((double) time / (double) this.WINDOW_TIME);
        double inputRateLocal = this.getInputRate().get(windowNumber).doubleValue() / ((double) time / (double) this.WINDOW_TIME);
        double inputMsgSize = this.getInputSize().get(windowNumber).doubleValue() / this.getInputRate().get(windowNumber).doubleValue();
        double outputMsgSize = this.getOutputSize().get(windowNumber).doubleValue() / this.getOutputRate().get(windowNumber).doubleValue();
        double processingTimeLocal = this.getProcessingTime().get(windowNumber).doubleValue() / this.getOutputRate().get(windowNumber).doubleValue();

        System.out.println("Application Sequence Id: " + sequenceId.get() +
                " Avg End-to-End App Latency (ms): " + meanLatency +
                " Min End-to-End App Latency (ms): " + min +
                " Max End-to-End App Latency (ms): " + max +
                " median End-to-End App Latency (ms): " + median +
                " Std End-to-End App Latency (ms): " + std +
                " End-to-End App Latency (regs): " + size +
                " Input Rate(Msg/s): " + inputRateLocal +
                " Output Rate (Msg/s): " + outputRateLocal +
                " Input Msg Size (bytes): " + inputMsgSize +
                " Output Msg Size (bytes): " + outputMsgSize +
                " Operator Time(ns): " + processingTimeLocal);

//        System.out.println(sequenceId.get() +
//                ";" + latencyLocal + ";" + windowNumber + ";" + time);

//        System.out.println("======== Operator ==========");
//        System.out.println(sequenceId.get() +
//                ";" + meanLatency + ";" + min + ";" + max + ";" + median + ";" + std + ";" + outputRateLocal + ";" + this.getOutputRate().get(windowNumber).doubleValue() + ";" + windowNumber);


        //Create tuple
        ClientMessage msg = this.getConnector().createMessage(false);
        msg.putStringProperty("timeStamp", String.valueOf(System.currentTimeMillis()));
        msg.putStringProperty("sequence_id", String.valueOf(sequenceId.get()));
        msg.putStringProperty("message_type", "application");
        msg.putStringProperty("topology_id", this.getTopologyId());
        msg.putStringProperty("node_id", this.getNodeId());
        msg.putStringProperty("throughput", String.valueOf(outputRateLocal));
        msg.putStringProperty("latency", String.valueOf(meanLatency));

        msg.putStringProperty("min_latency", String.valueOf(min));
        msg.putStringProperty("max_latency", String.valueOf(max));
        msg.putStringProperty("median_latency", String.valueOf(median));
        msg.putStringProperty("std_latency", String.valueOf(std));


        msg.putStringProperty("operator_id", this.getOperatorId());
        msg.putStringProperty("input_rate", String.valueOf(inputRateLocal));
        msg.putStringProperty("input_msg_size", String.valueOf(inputMsgSize));
        msg.putStringProperty("output_msg_size", String.valueOf(outputMsgSize));
        msg.putStringProperty("processing_time", String.valueOf(processingTimeLocal));
        msg.putStringProperty("tupleType", "metrics");
        msg.putStringProperty("batch_size", this.getBatchSize());
        msg.putStringProperty("buffer_consumer_size", this.getBufferConsumerSize());
        msg.putStringProperty("buffer_producer_size", this.getBufferProducerSize());

        byte[] originList = SerializationUtils.serialize(this.getOriginList());
        msg.putObjectProperty("tupleOriginID", originList);

        try {
            this.getActiveMQProducer().send(msg);

            commitCount.getAndIncrement();
            if (commitCount.get() == COMMIT_NUMBER) {
                this.getConnector().commit();
                commitCount.set(0);
            }
        } catch (ActiveMQException e) {
            e.printStackTrace();
        }

        sequenceId.getAndIncrement();
    }

    /**
     * The data stream application is structured as a DAG where operators are vertices and the streams are edges.
     * The metrics of both edge and vertex are computed according to windows. Each window stays active for @see #WINDOWTIME.
     * Once the @see #WINDOWTIME is exceeded then the method for publishing the metrics is called.
     * The metrics are computed per second, then the @time is converted accordingly.
     * This function computes each edge metrics.
     *
     * @param windowNumber - this is the number of the window where the metrics were stored.
     * @param time         - this is the time in ms when the metrics were accumulated.
     */
    public void publishEdgeMetrics(int windowNumber, long time) {
        for (String operators : this.getRegisteredOperators().get(windowNumber)) {
            //Compute metrics

            double max = Collections.max(this.getEdgeTransferringTime().get(windowNumber).get(operators)).doubleValue() / 1000000;
            double min = Collections.min(this.getEdgeTransferringTime().get(windowNumber).get(operators)).doubleValue() / 1000000;
//            print_stats_array(this.getEdgeTransferringTime().get(windowNumber).get(operators));

            Collections.sort(this.getEdgeTransferringTime().get(windowNumber).get(operators));
            int size = this.getEdgeTransferringTime().get(windowNumber).get(operators).size();
            int position = size / 2;
            double median = this.getEdgeTransferringTime().get(windowNumber).get(operators).get(position).doubleValue() / 1000000;
            double meantransferringTime = mean(this.getEdgeTransferringTime().get(windowNumber).get(operators)) / 1000000;
            double std = sd(this.getEdgeTransferringTime().get(windowNumber).get(operators), meantransferringTime);

//            double transferringTime = this.getEdgeTransferringTime().get(windowNumber).get(operators).doubleValue() / this.getEdgeTransferringRate().get(windowNumber).get(operators).doubleValue();
            double transferringSize = this.getEdgeTransferringSize().get(windowNumber).get(operators).doubleValue() / this.getEdgeTransferringRate().get(windowNumber).get(operators).doubleValue();
            double edgeRate = this.getEdgeTransferringRate().get(windowNumber).get(operators).doubleValue() / ((double) time / (double) this.WINDOW_TIME);


            System.out.println("Edge Sequence Id: " + this.getEdgeSequenceID().get(operators).get() +
                    " Mean Transferring Time (ms): " + meantransferringTime +
                    " Min Transferring Time (ms): " + min +
                    " Max Transferring Time (ms): " + max +
                    " Median Transferring Time (ms): " + median +
                    " Std Transferring Time (ms): " + std +
                    " Transferring Size (bytes): " + transferringSize);

            //Create tuple
            ClientMessage msg = this.getConnector().createMessage(false);
            msg.putStringProperty("timeStamp", String.valueOf(System.currentTimeMillis()));
            msg.putStringProperty("sequence_id", String.valueOf(this.getEdgeSequenceID().get(operators).get()));
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
            msg.putStringProperty("previous_operator_id", operators);
            msg.putStringProperty("batch_size", this.getBatchSize());
            msg.putStringProperty("buffer_consumer_size", this.getBufferConsumerSize());
            msg.putStringProperty("buffer_producer_size", this.getBufferProducerSize());
            msg.putStringProperty("tupleType", "metrics");

            try {
                this.getActiveMQProducer().send(msg);
            } catch (Exception e) {
                e.printStackTrace();
            }

            this.getEdgeSequenceID().get(operators).getAndIncrement();
        }
    }


    /**
     * If the operator requires to track the receiving tuples to check the consistency, then a message is sent an ArrayList
     * to the metrics system with all receiving tuples.
     *
     * @param windowNumber - this is the number of the window where the metrics were stored.
     */
    public void publishConsistencyMetrics(int windowNumber) {
        if (!this.getReceivingControl().get(windowNumber).isEmpty()) {
            //Create tuple
//            System.out.println("------>>> Number of receiving tuples: " + String.valueOf(this.getReceivingControl().get(windowNumber).size()));
            ClientMessage msg = this.getConnector().createMessage(false);
            msg.putStringProperty("timeStamp", String.valueOf(System.currentTimeMillis()));
            msg.putStringProperty("sequence_id", String.valueOf(sequenceId.get()));
            msg.putStringProperty("message_type", "consistency");
            msg.putStringProperty("topology_id", this.getTopologyId());
            msg.putStringProperty("node_id", this.getNodeId());
            msg.putStringProperty("operator_id", this.getOperatorId());
            msg.putStringProperty("tupleType", "metrics");
            msg.putStringProperty("batch_size", this.getBatchSize());
            msg.putStringProperty("buffer_consumer_size", this.getBufferConsumerSize());
            msg.putStringProperty("buffer_producer_size", this.getBufferProducerSize());

            byte[] receivingList = SerializationUtils.serialize(this.getReceivingControl().get(windowNumber));
            msg.putObjectProperty("receiving_list", receivingList);

            try {
                this.getActiveMQProducer().send(msg);
            } catch (ActiveMQException e) {
                e.printStackTrace();
            }
        }

    }

    /**
     * This functions publishes the state metrics.
     *
     * @param windowNumber - this is the number of the window where the metrics were stored.
     */
    public void publishStateMetrics(int windowNumber, long time) {
        if (!this.getStateControl().get(windowNumber).isEmpty()) {
            ClientMessage msg = this.getConnector().createMessage(false);
            msg.putStringProperty("timeStamp", String.valueOf(System.currentTimeMillis()));
            msg.putStringProperty("sequence_id", String.valueOf(sequenceId.get()));
            msg.putStringProperty("message_type", "OPERATOR_STATE");
            msg.putStringProperty("topology_id", this.getTopologyId());
            msg.putStringProperty("node_id", this.getNodeId());
            msg.putStringProperty("operator_id", this.getOperatorId());
            msg.putStringProperty("tupleType", "metrics");
            msg.putStringProperty("batch_size", this.getBatchSize());
            msg.putStringProperty("buffer_consumer_size", this.getBufferConsumerSize());
            msg.putStringProperty("buffer_producer_size", this.getBufferProducerSize());

            for (Map.Entry<String, StateValues> entry : this.getStateControl().get(windowNumber).getStateCollection().entrySet()) {
                entry.getValue().setRate(entry.getValue().getCounter() / ((double) time / (double) this.WINDOW_TIME));
            }

            byte[] state_collection = SerializationUtils.serialize(this.getStateControl().get(windowNumber));
            msg.putObjectProperty("state_collection", state_collection);

            try {
                this.getActiveMQProducer().send(msg);
            } catch (ActiveMQException e) {
                e.printStackTrace();
            }
        }

    }

    /**
     * This method calls all functions to publish the metrics.
     *
     * @param windowNumber - this is the number of the window where the metrics were stored.
     * @param time         - this is the time in ms when the metrics were accumulated.
     */
    private void publishMetrics(int windowNumber, long time) {

        if (this.isCheckConsistency()) {
            this.publishConsistencyMetrics(windowNumber);
        }

        if (this.isCheckState()) {
            this.publishStateMetrics(windowNumber, time);
        }

        this.publishEdgeMetrics(windowNumber, time);
        this.publishOperatorMetric(windowNumber, time);
    }


    /*
        Methods to include information to the application metric system
     */

    /**
     * The data stream application is structured as a DAG where operators are vertices and the streams are edges.
     * This method stores information of a tuple (when it was created by the producer and when it arrived to this current operator),
     * and it also computes metrics for each edge of the DAG.
     *
     * @param msgID              - this is the tuple timestamp, which is used as ID. The method gets the ID to lookup a table
     *                           and identifies the time, where the tuple was generated by the producer and the time when it arrived in the current operator.
     * @param msgTimeCreation    - time when the tuple was created by the producer
     * @param event_size         - this is the size of the tuple (bytes) when it arrived in the operator before the operator transformation
     * @param previousOperatorId - this is the source operator of the stream
     * @param transferringTime   - this is the time required to transfer the message from the source to the destination of the stream
     * @param key                - this is the key used to control stateful operations
     * @param liveTuple
     */
    public void AddInputEventSize(String msgID, String msgTimeCreation, Long event_size, String previousOperatorId, String transferringTime, String key, boolean liveTuple) {

        if (this.getPublish().getState() == Thread.State.WAITING) {
            synchronized (this.getPublish()) {
                this.getPublish().notify();
            }
        }
        if (liveTuple) {

            lastTupleReceiving.set(ZonedDateTime.now());
            synchronized (lock) {
                //Operator metric system
                if (!this.getFirstEndToEndLatency().containsKey(key)) {
                    this.getFirstEndToEndLatency().put(key, ZonedDateTime.parse(msgTimeCreation));
                }
//        System.out.println(this.getPublish().getState());
//        System.out.println(msgID);
                lastTupleReceiving.set(ZonedDateTime.now());
                synchronized (lock) {
                    //Operator metric system
                    if (!this.getFirstEndToEndLatency().containsKey(msgID)) {
                        this.getFirstEndToEndLatency().put(msgID, ZonedDateTime.parse(msgTimeCreation));
                    }

                    if (!this.getFirstTupleReceiving().containsKey(msgID)) {
                        this.getFirstTupleReceiving().put(msgID, ZonedDateTime.now());
                    }

                    if (!this.getFirstTupleReceiving().containsKey(key)) {
                        this.getFirstTupleReceiving().put(key, ZonedDateTime.now());
                    }


                    //Edge metric system
                    Duration duration = Duration.between(ZonedDateTime.parse(transferringTime), ZonedDateTime.now());
                    Long transferTime = duration.toNanos();
                    this.AddEdgeMetrics(previousOperatorId, transferTime, event_size);

                    this.getInputSize().put(activeWindowNumber.get(), this.getInputSize().get(activeWindowNumber.get()) + event_size);
                    this.getInputRate().put(activeWindowNumber.get(), this.getInputRate().get(activeWindowNumber.get()) + 1);

                    //Update last sending
                    if (this.getFirstReceiving().get(activeWindowNumber.get()) == null) {
                        this.getFirstReceiving().put(activeWindowNumber.get(), ZonedDateTime.now());
                    }

                    //Update state
                    if (key != null && key != "") {
                        this.setCheckState(true);
                        if (this.getStateControl().get(activeWindowNumber.get()).containsKey(key)) {
                            this.getStateControl().get(activeWindowNumber.get()).getKey(key).setCounter(this.getStateControl().get(activeWindowNumber.get()).getKey(key).getCounter() + 1);
                        } else {
                            this.getStateControl().get(activeWindowNumber.get()).put(key, new StateValues(key, this.getKeyType(), 1, 1));
                        }
                    }
                }
            }
        }
    }

    /**
     * This function deletes old message IDs or it clears the end to end latency and operator processing time variables.
     *
     * @param msgID - internal id used to control arrivals
     */
    public void deleteMsgId(String msgID) {

        this.getFirstTupleReceiving().clear();
        this.getFirstEndToEndLatency().clear();
    }

    /**
     * The data stream application is structured as a DAG where operators are vertices and the streams are edges.
     * This methods computes the values of processing time, output rate and output tuple size for a DAG vertex.
     *
     * @param msgID      - this is the tuple timestamp, which is used as ID. The method gets the ID to lookup a table
     *                   and identifies the time where the tuple was generated by the producer and the time when it arrived in the current operator.
     * @param event_size - this is the output size (bytes) of the tuple after passing by the operator transformation.
     * @param inputKey   - key used to create the state
     * @param liveTuple
     */

    public void AddOutputEventSize(String msgID, Long event_size, String inputKey, String timeStampFromSource, boolean liveTuple) {

        if (liveTuple) {
            synchronized (lock) {
                //Processing time

                //Old version for computing the processing time
//            Duration processingTime = Duration.between(this.getTimestampMsgArrival().get(msgID), ZonedDateTime.now());
//            this.getProcessingTime().put(activeWindowNumber.get(), this.getProcessingTime().get(activeWindowNumber.get()) + (double) processingTime.toNanos());
                //            if (this.deleteInternalID.get()) {
//                this.getTimestampMsgArrival().remove(msgID);
//            }

                //New version for computing the processing time
                if (this.getProcessingTime().containsKey(activeWindowNumber.get())) {
                    if (!this.getFirstTupleReceiving().isEmpty()) {
                        if (this.getFirstTupleReceiving().containsKey(msgID)) {
                            this.getProcessingTime().put(activeWindowNumber.get(), this.getProcessingTime().get(activeWindowNumber.get()) + Duration.between(this.getFirstTupleReceiving().get(msgID), ZonedDateTime.now()).toNanos());
                        }
                    }
                }
                //this comparison is to check the time consistency
//            double processinTime1 = Duration.between(this.getTimestampMsgArrival().get(msgID), znow).toNanos();
//            double processinTime2 = Duration.between(this.getFirstTupleReceiving().get(inputKey), znow).toNanos();
////            System.out.println("PTime 1: " + String.valueOf(processinTime1) + " PTime 2: " + String.valueOf(processinTime2));

                if (this.deleteInternalID.get()) {
                    this.getFirstTupleReceiving().remove(msgID);
                }

                //End-to-End Latency
                //Old version for computing the End-to-End Latency
//            Duration endToEnd = Duration.between(this.getTimestampMsgCreation().get(msgID), ZonedDateTime.now());
//            this.getEndToEndLatency().put(activeWindowNumber.get(), this.getEndToEndLatency().get(activeWindowNumber.get()) + (double) endToEnd.toMillis());
                //            if (this.deleteInternalID.get()) {
//                this.getTimestampMsgCreation().remove(msgID);
//            }

                //New version for computing the End-to-End Latency
//            this.getEndToEndLatency().put(activeWindowNumber.get(), this.getEndToEndLatency().get(activeWindowNumber.get()) + (double) Duration.between(this.getFirstEndToEndLatency().get(inputKey), ZonedDateTime.now()).toMillis());
                Long timeLat = Duration.between(ZonedDateTime.parse(timeStampFromSource), ZonedDateTime.now()).toNanos();
                this.getEndToEndLatency().get(activeWindowNumber.get()).add(timeLat);
//            System.out.println(msgID + ";" + timeLat);
                //this comparison is to check the time consistency
//            double e1 = Duration.between(this.getTimestampMsgCreation().get(msgID), ZonedDateTime.now()).toMillis();
//            double e2 = Duration.between(this.getFirstEndToEndLatency().get(inputKey), znow).toMillis();
//            System.out.println("Time 1: " + String.valueOf(e1) + " Time 2: " + String.valueOf(e2));

                if (this.deleteInternalID.get()) {
                    this.getFirstEndToEndLatency().remove(msgID);
                }


//                //New version for computing the processing time
//                this.getProcessingTime().put(activeWindowNumber.get(), this.getProcessingTime().get(activeWindowNumber.get()) + Duration.between(this.getFirstTupleReceiving().get(inputKey), ZonedDateTime.now()).toNanos());
//
//                //this comparison is to check the time consistency
//                if (this.deleteInternalID.get()) {
//                    this.getFirstTupleReceiving().remove(inputKey);
//                }

                //New version for computing the End-to-End Latency
//                timeLat = Duration.between(ZonedDateTime.parse(timeStampFromSource), ZonedDateTime.now()).toNanos();
//                this.getEndToEndLatency().get(activeWindowNumber.get()).add(timeLat);

//                if (this.deleteInternalID.get()) {
//                    this.getFirstEndToEndLatency().remove(inputKey);
//                }

                //Output size and output rate
                this.getOutputSize().put(activeWindowNumber.get(), this.getOutputSize().get(activeWindowNumber.get()) + event_size);
                this.getOutputRate().put(activeWindowNumber.get(), this.getOutputRate().get(activeWindowNumber.get()) + 1);

                //Update last sending
                this.getLastSending().put(activeWindowNumber.get(), ZonedDateTime.now());

                //Consistency
                if (this.isCheckConsistency()) {
                    this.getReceivingControl().get(activeWindowNumber.get()).add(msgID);
                }

            }
        }

    }

    /**
     * The data stream application is structured as a DAG where operators are vertices and the streams are edges.
     * This function estimates the average transferring time, average message sizes, and processing rate of each edge.
     * If the variables of the current window are not initiated, then the method does so.
     *
     * @param previousOperatorId - this is the source of the edge
     * @param transferTime       - time required to transfer the tuple from the previous operator to this current operator
     * @param transferData       - size of the message that arrived in the edge
     */
    public void AddEdgeMetrics(String previousOperatorId, Long transferTime, Long transferData) {
        //Initiate the registered operators
        if (!this.getRegisteredOperators().containsKey(activeWindowNumber.get())) {
            this.getRegisteredOperators().put(activeWindowNumber.get(), new ArrayList<>());
            this.getRegisteredOperators().get(activeWindowNumber.get()).add(previousOperatorId);
        }


        //Initiate the edge sequence id
        if (!this.getEdgeSequenceID().containsKey(previousOperatorId)) {
            this.getEdgeSequenceID().put(previousOperatorId, new AtomicLong(0));
        }

        //Transferring time
        if (!this.getEdgeTransferringTime().containsKey(activeWindowNumber.get())) {
            HashMap<String, ArrayList<Long>> inner = new HashMap<>();
            ArrayList<Long> tt = new ArrayList<>();
            tt.add(transferTime);
            inner.put(previousOperatorId, tt);
            this.getEdgeTransferringTime().put(activeWindowNumber.get(), inner);

        } else {
            if (!this.getEdgeTransferringTime().get(activeWindowNumber.get()).containsKey(previousOperatorId)) {
                ArrayList<Long> aux = new ArrayList<>();
                aux.add(transferTime);
                this.getEdgeTransferringTime().get(activeWindowNumber.get()).put(previousOperatorId, aux);
            } else {
                this.getEdgeTransferringTime().get(activeWindowNumber.get()).get(previousOperatorId).add(transferTime);
            }

        }

        //Amount of transferred data
        if (!this.getEdgeTransferringSize().containsKey(activeWindowNumber.get())) {
            HashMap<String, Long> inner = new HashMap<>();
            inner.put(previousOperatorId, transferData);
            this.getEdgeTransferringSize().put(activeWindowNumber.get(), inner);
        } else {
            Long value = 0l;
            if (this.getEdgeTransferringSize().get(activeWindowNumber.get()).containsKey(previousOperatorId)) {
                value = this.getEdgeTransferringSize().get(activeWindowNumber.get()).get(previousOperatorId);
            }

            this.getEdgeTransferringSize().get(activeWindowNumber.get()).put(previousOperatorId, value + transferData);
        }


        //Processing rate (number of messages)
        if (!this.getEdgeTransferringRate().containsKey(activeWindowNumber.get())) {
            HashMap<String, Long> inner = new HashMap<>();
            inner.put(previousOperatorId, 1l);
            this.getEdgeTransferringRate().put(activeWindowNumber.get(), inner);
        } else {
            Long value = 0l;
            if (this.getEdgeTransferringRate().get(activeWindowNumber.get()).containsKey(previousOperatorId)) {
                value = this.getEdgeTransferringRate().get(activeWindowNumber.get()).get(previousOperatorId);
            }
            this.getEdgeTransferringRate().get(activeWindowNumber.get()).put(previousOperatorId, value + 1);
        }
    }

    /**
     * Prepare to destroy MetricsCollector2. First the publishing thread is terminated. Second, the metrics are flushed
     * to ActiveMQ. And finally, the ActiveMQ connection is closed.
     */
    public void close() {
        //Signal to stop the thread publishing thread
        running.set(false);
        //Wait until thread is closed
        while (!closed.get()) {

        }

        //Publish current values
        if (this.getInputRate().get(activeWindowNumber.get()) > 0) {
            synchronized (lock) {
                /*
                Computes the time since the window was created
                 */
                ZonedDateTime s = getTimestampWindowCreation().get(activeWindowNumber.get());
                Duration duration = Duration.between(s, ZonedDateTime.now());
                long time = duration.toMillis();

                //Gets the active window
                int windowNumber = activeWindowNumber.get();

                this.publishMetrics(windowNumber, time);

            }
        }

        try {
            //Commit current connection
            this.getConnector().commit();

            //Close connection
            this.getActiveMQProducer().close();
            this.getConnector().close();
        } catch (ActiveMQException e) {
            e.printStackTrace();
        }
    }

    //Gets and sets
    public Thread getPublish() {
        return publish;
    }

    public void setPublish(Thread publish) {
        this.publish = publish;
    }

    /*
        Metric system
     */
    public HashMap<Integer, Long> getInputRate() {
        return inputRate;
    }

    public void setInputRate(HashMap<Integer, Long> inputRate) {
        this.inputRate = inputRate;
    }

    public HashMap<Integer, Long> getOutputRate() {
        return outputRate;
    }

    public void setOutputRate(HashMap<Integer, Long> outputRate) {
        this.outputRate = outputRate;
    }

    public HashMap<Integer, Long> getInputSize() {
        return inputSize;
    }

    public void setInputSize(HashMap<Integer, Long> inputSize) {
        this.inputSize = inputSize;
    }

    public HashMap<Integer, Long> getOutputSize() {
        return outputSize;
    }

    public void setOutputSize(HashMap<Integer, Long> outputSize) {
        this.outputSize = outputSize;
    }

    public HashMap<Integer, ArrayList<Long>> getEndToEndLatency() {
        return endToEndLatency;
    }

    public void setEndToEndLatency(HashMap<Integer, ArrayList<Long>> endToEndLatency) {
        this.endToEndLatency = endToEndLatency;
    }

    public HashMap<Integer, Long> getProcessingTime() {
        return processingTime;
    }

    public void setProcessingTime(HashMap<Integer, Long> processingTime) {
        this.processingTime = processingTime;
    }


    /*
        Support system
     */
//    public HashMap<String, ZonedDateTime> getTimestampMsgCreation() {
//        return timestampMsgCreation;
//    }
//
//    public void setTimestampMsgCreation(HashMap<String, ZonedDateTime> timestampMsgCreation) {
//        this.timestampMsgCreation = timestampMsgCreation;
//    }

//    public HashMap<String, ZonedDateTime> getTimestampMsgArrival() {
//        return timestampMsgArrival;
//    }
//
//    public void setTimestampMsgArrival(HashMap<String, ZonedDateTime> timestampMsgArrival) {
//        this.timestampMsgArrival = timestampMsgArrival;
//    }

    public HashMap<Integer, ZonedDateTime> getLastSending() {
        return lastSending;
    }

    public void setLastSending(HashMap<Integer, ZonedDateTime> lastSending) {
        this.lastSending = lastSending;
    }

    public HashMap<Integer, ZonedDateTime> getFirstReceiving() {
        return firstReceiving;
    }

    public void setFirstReceiving(HashMap<Integer, ZonedDateTime> firstReceiving) {
        this.firstReceiving = firstReceiving;
    }

    public HashMap<Integer, StateCollection> getStateControl() {
        return stateControl;
    }

    public void setStateControl(HashMap<Integer, StateCollection> stateControl) {
        this.stateControl = stateControl;
    }


    /*
        Metric management
     */
    public HashMap<Integer, ZonedDateTime> getTimestampWindowCreation() {
        return timestampWindowCreation;
    }

    public void setTimestampWindowCreation(HashMap<Integer, ZonedDateTime> timestampWindowCreation) {
        this.timestampWindowCreation = timestampWindowCreation;
    }


    /*
        ActiveMQ connection
     */
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


    /*
        Gets/sets for the operator execution
     */
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

    public ArrayList<String> getOriginList() {
        return originList;
    }

    public void setOriginList(ArrayList<String> originList) {
        this.originList = originList;
    }

    public boolean isCheckConsistency() {
        return checkConsistency;
    }

    public void setCheckConsistency(boolean checkConsistency) {
        this.checkConsistency = checkConsistency;
    }


    public boolean isCheckState() {
        return checkState;
    }

    public void setCheckState(boolean checkState) {
        this.checkState = checkState;
    }


    public HashMap<String, ZonedDateTime> getFirstEndToEndLatency() {
        return firstEndToEndLatency;
    }

    public void setFirstEndToEndLatency(HashMap<String, ZonedDateTime> firstEndToEndLatency) {
        this.firstEndToEndLatency = firstEndToEndLatency;
    }

    public HashMap<String, ZonedDateTime> getFirstTupleReceiving() {
        return firstTupleReceiving;
    }

    public void setFirstTupleReceiving(HashMap<String, ZonedDateTime> firstTupleReceiving) {
        this.firstTupleReceiving = firstTupleReceiving;
    }

    /*
        Gets/sets edges metric system
     */
    public HashMap<Integer, ArrayList<String>> getRegisteredOperators() {
        return registeredOperators;
    }

    public void setRegisteredOperators(HashMap<Integer, ArrayList<String>> registeredOperators) {
        this.registeredOperators = registeredOperators;
    }

    public HashMap<Integer, HashMap<String, Long>> getEdgeTransferringSize() {
        return edgeTransferringSize;
    }

    public void setEdgeTransferringSize(HashMap<Integer, HashMap<String, Long>> edgeTransferringSize) {
        this.edgeTransferringSize = edgeTransferringSize;
    }

    public HashMap<Integer, HashMap<String, ArrayList<Long>>> getEdgeTransferringTime() {
        return edgeTransferringTime;
    }

    public void setEdgeTransferringTime(HashMap<Integer, HashMap<String, ArrayList<Long>>> edgeTransferringTime) {
        this.edgeTransferringTime = edgeTransferringTime;
    }

    public HashMap<Integer, HashMap<String, Long>> getEdgeTransferringRate() {
        return edgeTransferringRate;
    }

    public void setEdgeTransferringRate(HashMap<Integer, HashMap<String, Long>> edgeTransferringRate) {
        this.edgeTransferringRate = edgeTransferringRate;
    }

    public HashMap<String, AtomicLong> getEdgeSequenceID() {
        return edgeSequenceID;
    }

    public void setEdgeSequenceID(HashMap<String, AtomicLong> edgeSequenceID) {
        this.edgeSequenceID = edgeSequenceID;
    }


    public String getKeyType() {
        return keyType;
    }

    public void setKeyType(String keyType) {
        this.keyType = keyType;
    }


    //Operator consistency
    public HashMap<Integer, ArrayList<String>> getReceivingControl() {
        return receivingControl;
    }

    public void setReceivingControl(HashMap<Integer, ArrayList<String>> receivingControl) {
        this.receivingControl = receivingControl;
    }
}