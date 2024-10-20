package com.edgestream.worker.metrics.metricscollector2;

import com.edgestream.worker.metrics.metricscollector2.operator.OperatorCollector;
import com.edgestream.worker.metrics.metricscollector2.reconfiguration.ReconfigurationCollector;
import com.edgestream.worker.metrics.metricscollector2.state.StateCollector;
import com.edgestream.worker.metrics.metricscollector2.statemanagement.StateManagementCollector;
import com.edgestream.worker.metrics.metricscollector2.stream.StreamCollector;
import com.edgestream.worker.metrics.metricscollector2.utils.GenericCollector;
import org.apache.activemq.artemis.api.core.client.*;

import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

/***
 * @author Alexandre da Silva Veith
 * The MetricsCollector3 computes metrics from operators, operators' states (stateful),  streams, reconfiguration,
 * and state management. Shepherd structures data stream applications DAGs where operators are vertices and streams
 * are edges. Each operator assignment runs along with a MetricsCollector3 service (thread) to collect execution
 * metrics. MetricsCollector3 iterates over a collection of collectors, which extend the generic class
 * {@link GenericCollector}. Below the existing collectors:
 * - {@link OperatorCollector} Operator function statistics;
 * - {@link StreamCollector} Stream statistics;
 * - {@link StateCollector} Window/keyed state statistics;
 * - {@link StateManagementCollector} Statistics of the state management to carry out operation such as checkpointing,
 *                                    on-demand backup, and restore;
 * - {@link ReconfigurationCollector} Reconfiguration logs for receiving the stable and reconfiguration markers;
 * The constructor {@link MetricsCollector3} feeds an ArrayList with the collectors for stateless operators. Stateful
 * operators require calling the function {@link MetricsCollector3#setStatefulOperator()}, which adds the collectors to
 * the set of collectors. These collectors are monitored by a thread that runs every
 * {@link MetricsCollector3#FREQUENCY_PUBLISH} ms. The thread verifies whether the collectors are ready to publish
 * their metrics by interacting with the "isReady" function from each collector. The "publish" method of the collectors
 * send the metrics to an Artemis connector. The connector does not push immediately the metrics, it holds them until
 * the number of sends reach a goal {@link MetricsCollector3#COMMIT_NUMBER}. The {@link MetricsCollector3#close()}
 * forces the closing of each of the collectors by sending the metrics before the window closes.
 *
 * Procedure to add new collectors:
 * 1) Create a new measure class extending {@link com.edgestream.worker.metrics.metricscollector2.utils.GenericMeasure};
 * 2) Implement a new collector extending {@link GenericCollector} to fill the new measure
 * class (e.g.,{@link StreamCollector});
 * 3) The Artemis metric message must contain one static system property ("tupleType", "metrics") and another with the
 * metric name ("message_type", "state_management") - the remaining properties vary according to the desired metrics;
 * 4) This new metric travels to a service that run on the cloud "MetricsMonitor". This service requires the schema of
 *  the new metric {@link com.edgestream.worker.metrics.cloudDB.CloudMetricsDB#CloudMetricsDB}
 *  (e.g., {com.edgestream.worker.metrics.cloudDB.CloudMetricsDB#createApplicationMetricsSchema()});
 * 5) When the metric message arrives at the Metrics Montior, a parser
 * {@link com.edgestream.worker.metrics.cloudDB.CloudMetricsMessageParser#parseMetricMessage(ClientMessage clientMessage)}
 * determines in which one of the schemas the metrics must be saved. This requires to register the metric
 * to {@link com.edgestream.worker.metrics.model.MetricType};
 * 6) The parser calls the method "create metrics", which requires to create a new object of the type of the metric
 * (e.g., {@link com.edgestream.worker.metrics.model.ApplicationMetric}). This object is sent as an argument to a "put"
 * method found in {@link com.edgestream.worker.metrics.cloudDB.CloudMetricsDBWriter}.
 */

public class MetricsCollector3 {
    Thread publish;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicBoolean closed = new AtomicBoolean(false);

    //ActiveMQ connection
    private ClientProducer activeMQProducer;
    private ClientSession connector;

    //Collectors
    private ArrayList<GenericCollector> collectors;

    final int COMMIT_NUMBER = 1;
    final int FREQUENCY_PUBLISH = 1000;

    /**
     * The constructor creates an ActiveMQ connection for publishing metric tuples using @param metrics_server, and @param fqqn.
     * Each metric tuple has a label to identify the @param topology_id, @param node_id, @param operator_id,
     * and @param operator_id. The label also contains details on how the ActiveMQ producer and consumer were configured
     * (@param buffer_consumer_size, @param buffer_producer_size, and @param batch_size).
     * A thread controls when to publish metrics.
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
    public MetricsCollector3(String metrics_server, String fqqn, String topology_id, String node_id, String operator_id, String buffer_consumer_size, String buffer_producer_size, String batch_size) {

        //Create ActiveMQ Producer
        if (metrics_server != null) {
            this.setActiveMQProducer(this.createLocalProducer(metrics_server, fqqn));
        }

        this.setCollectors(new ArrayList<>());
        this.getCollectors().add(new OperatorCollector(this.getConnector(), this.getActiveMQProducer(), topology_id, node_id, operator_id, buffer_consumer_size, buffer_producer_size, batch_size));
        this.getCollectors().add(new StreamCollector(this.getConnector(), this.getActiveMQProducer(), topology_id, node_id, operator_id, buffer_consumer_size, buffer_producer_size, batch_size));
        this.getCollectors().add(new ReconfigurationCollector(this.getConnector(), this.getActiveMQProducer(), topology_id, node_id, operator_id, buffer_consumer_size, buffer_producer_size, batch_size));

        //Start publishing thread
        this.createControlThread();
    }

    /**
     * This function creates the collectors for stateful operators
     */
    public void setStatefulOperator() {
        this.getCollectors().add(new StateManagementCollector(this.getConnector(), this.getActiveMQProducer(), this.getOperatorCollector().getTopologyId(), this.getOperatorCollector().getNodeId(), this.getOperatorCollector().getOperatorId(), this.getOperatorCollector().getBufferConsumerSize(), this.getOperatorCollector().getBufferProducerSize(), this.getOperatorCollector().getBatchSize()));
        this.getCollectors().add(new StateCollector(this.getConnector(), this.getActiveMQProducer(), this.getOperatorCollector().getTopologyId(), this.getOperatorCollector().getNodeId(), this.getOperatorCollector().getOperatorId(), this.getOperatorCollector().getBufferConsumerSize(), this.getOperatorCollector().getBufferProducerSize(), this.getOperatorCollector().getBatchSize()));
    }


    /**
     * A thread controls when publishing metrics.
     * The thread verifies whether the collectors are ready to publish
     * their metrics by interacting with the "isReady" function from each collector. The "publish" method of the collectors
     * send the metrics to an Artemis connector. The connector does not push immediately the metrics, it holds them until
     * the number of sends reach a goal {@link MetricsCollector3#COMMIT_NUMBER}.
     */
    private void createControlThread() {
        this.setPublish(new Thread(() -> {
            running.set(true);

            try {
                int rounds = 0;
                boolean commits = false;
                while (running.get()) {
                    boolean sessionUpdated = false;
                    for (int i = 0; i < this.getCollectors().size(); i++) {
//                        System.out.println("Calling collector " + i);
                        if (this.getCollectors().get(i).isReady()) {
                            sessionUpdated = this.getCollectors().get(i).publish(false);
                        }
                        if (!commits && sessionUpdated) {
                            commits = true;
                        }
//                        System.out.println("Completing collector " + i + " with sessionUpdated: " + sessionUpdated);
                    }
                    Thread.sleep(FREQUENCY_PUBLISH);
                    rounds++;

                    if (rounds >= COMMIT_NUMBER && commits) {
                        this.getConnector().commit();
                        commits = false;
                        rounds = 0;
                    }
                }

            } catch (Exception v) {
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
     * Prepare to destroy MetricsCollector2. First the publishing thread is terminated. Second, the metrics are flushed
     * to ActiveMQ. And finally, the ActiveMQ connection is closed.
     */
    public void close() {
        //Signal to stop the thread publishing thread
        running.set(false);

        //Wait until thread is closed
        while (!closed.get()) {

        }

        try {
            boolean commits = false;
            boolean sessionUpdated = false;
            for (int i = 0; i < this.getCollectors().size(); i++) {
                sessionUpdated = this.getCollectors().get(i).close();

                if (!commits && sessionUpdated) {
                    commits = true;
                }
            }

            if (commits) {
                //Commit current connection
                this.getConnector().commit();
            }

            //Close connection
            this.getActiveMQProducer().close();
            this.getConnector().close();
        } catch (Exception e) {
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

    public ArrayList<GenericCollector> getCollectors() {
        return collectors;
    }

    public void setCollectors(ArrayList<GenericCollector> collectors) {
        this.collectors = collectors;
    }
    /*
        Gets/sets collectors
     */

    /** Wrappers to get the collectors in the ArrayList
     *
     * @return collectors
     */
    public OperatorCollector getOperatorCollector() {
        for (int i = 0; i < this.getCollectors().size(); i++) {
            if (this.getCollectors().get(i) instanceof OperatorCollector) {
                return (OperatorCollector) this.getCollectors().get(i);
            }
        }
        return null;
    }

    public StreamCollector getStreamCollector() {
        for (int i = 0; i < this.getCollectors().size(); i++) {
            if (this.getCollectors().get(i) instanceof StreamCollector) {
                return (StreamCollector) this.getCollectors().get(i);
            }
        }
        return null;
    }


    public StateCollector getStateCollector() {
        for (int i = 0; i < this.getCollectors().size(); i++) {
            if (this.getCollectors().get(i) instanceof StateCollector) {
                return (StateCollector) this.getCollectors().get(i);
            }
        }
        return null;
    }

    public StateManagementCollector getStateManagementCollector() {
        for (int i = 0; i < this.getCollectors().size(); i++) {
            if (this.getCollectors().get(i) instanceof StateManagementCollector) {
                return (StateManagementCollector) this.getCollectors().get(i);
            }
        }
        return null;
    }

    public ReconfigurationCollector getReconfigurationCollector() {
        for (int i = 0; i < this.getCollectors().size(); i++) {
            if (this.getCollectors().get(i) instanceof ReconfigurationCollector) {
                return (ReconfigurationCollector) this.getCollectors().get(i);
            }
        }
        return null;
    }


}