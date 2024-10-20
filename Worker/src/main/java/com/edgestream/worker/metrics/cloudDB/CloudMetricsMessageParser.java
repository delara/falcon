package com.edgestream.worker.metrics.cloudDB;

import com.edgestream.worker.config.EdgeStreamGetPropertyValues;
import com.edgestream.worker.metrics.db.pathstore.CloudMetricsPathstoreDBWritter;
import com.edgestream.worker.metrics.exception.EdgeStreamMetricsMessageException;
import com.edgestream.worker.metrics.model.*;
import com.edgestream.worker.runtime.network.DataCenterManager;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.commons.lang3.SerializationUtils;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;


public class CloudMetricsMessageParser {

    DataCenterManager dataCenterManager;
    CloudMetricsDBWriter cloudMetricsDBWriter;
    CloudMetricsDBReader cloudMetricsDBReader;
    CloudMetricsPathstoreDBWritter cloudMetricsPathstoreDBWritter;
    BufferedWriter writer;

    public CloudMetricsMessageParser() {
    }

    public CloudMetricsMessageParser(DataCenterManager dataCenterManager) {

        this.dataCenterManager = dataCenterManager;
        // Setup Derby DB
        this.cloudMetricsDBWriter = new CloudMetricsDBWriter(dataCenterManager.getMetricsDB());
        this.cloudMetricsDBReader = new CloudMetricsDBReader(dataCenterManager.getMetricsDB());

        // Setup PathStore DB
        if (dataCenterManager.getMetricsPathstoreDB() != null) {
            this.cloudMetricsPathstoreDBWritter = new CloudMetricsPathstoreDBWritter(dataCenterManager.getMetricsPathstoreDB());
        } else {
            this.cloudMetricsPathstoreDBWritter = null;
        }

        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy_MM_dd_HH_mm_ss");
        LocalDateTime now = LocalDateTime.now();
        //String fileName = "C:\\log\\edgestream.log" + "_" + dtf.format(now);
        String fileName = EdgeStreamGetPropertyValues.getMETRICS_LOG_PATH() + "edgestream" + "_" + dtf.format(now) + ".log";

        try {
            writer = new BufferedWriter(new FileWriter(fileName, true));
            writeToEdgeStreamLog("----------------------------Metrics-------------------------------");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public CloudMetricsDBReader getMetricsDBReader() {
        return this.cloudMetricsDBReader;
    }

    public void writeToEdgeStreamLog(String str) {

        try {
            writer.append(str);
            writer.append('\n');
            writer.flush();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public Metric parseMetricMessage(ClientMessage clientMessage) {

        if (clientMessage.getStringProperty("message_type").equalsIgnoreCase(MetricType.APPLICATION.toString())) {

            //System.out.print("Received an application metric:   ");

            ApplicationMetric applicationMetric = this.createApplicationMetric(clientMessage);
            writeToEdgeStreamLog("Received an application metric:   " + applicationMetric.toTuple());
            return applicationMetric;

        } else if (clientMessage.getStringProperty("message_type").equalsIgnoreCase(MetricType.DOCKER.toString())) {

            //System.out.print("Received a docker system metric:  ");

            DockerSystemMetric dockerSystemMetric = this.createDockerSystemMetric(clientMessage);
            writeToEdgeStreamLog("Received a docker system metric:  " + dockerSystemMetric.toTuple());
            return dockerSystemMetric;

        } else if (clientMessage.getStringProperty("message_type").equalsIgnoreCase(MetricType.SYSTEM.toString())) {

            //System.out.print("Received a hardware system metric:    ");
            HardwareSystemMetric hardwareSystemMetric = this.createHardwareSystemMetric(clientMessage);
            writeToEdgeStreamLog("Received a hardware system metric:    " + hardwareSystemMetric.toTuple());
            return hardwareSystemMetric;

        } else if (clientMessage.getStringProperty("message_type").equalsIgnoreCase(MetricType.NETWORK.toString())) {

            //System.out.print("Received a network system metric:     ");
            NetworkSystemMetric networkSystemMetric = this.createNetworkSystemMetric(clientMessage);
            writeToEdgeStreamLog("Received a network system metric:     " + networkSystemMetric.toTuple());
            return networkSystemMetric;


        } else if (clientMessage.getStringProperty("message_type").equalsIgnoreCase(MetricType.APPLICATION_EDGES.toString())) {

            //System.out.print("Received an application edge metric:     ");
            ApplicationEdgeMetric applicationEdgeMetric = this.createApplicationEdgeMetric(clientMessage);
            writeToEdgeStreamLog("Received an application edge metric:     " + applicationEdgeMetric.toTuple());
            return applicationEdgeMetric;
        } else if (clientMessage.getStringProperty("message_type").equalsIgnoreCase(MetricType.TRAFFIC.toString())) {

            TrafficMetric trafficMetric = this.createTrafficMetric(clientMessage);
            writeToEdgeStreamLog("Received a traffic metric:     " + trafficMetric.toTuple());
            return trafficMetric;

        } else if (clientMessage.getStringProperty("message_type").equalsIgnoreCase(MetricType.APPLICATION_STATE.toString())) {

            //System.out.print("Received an application edge metric:     ");
            ApplicationStateMetric applicationStateMetric = this.createApplicationStateMetric(clientMessage);
            writeToEdgeStreamLog("Received a application state metric:     " + applicationStateMetric.toTuple());
            return applicationStateMetric;

        } else if (clientMessage.getStringProperty("message_type").equalsIgnoreCase(MetricType.RECONFIGURATION.toString())) {

            ReconfigurationMetric reconfigurationMetric = this.createReconfigurationMetric(clientMessage);
            writeToEdgeStreamLog("Received a reconfiguration metric:     " + reconfigurationMetric.toTuple());
            return reconfigurationMetric;

        } else if (clientMessage.getStringProperty("message_type").equalsIgnoreCase(MetricType.STATE_MANAGEMENT.toString())) {

            StateManagementMetric stateManagementMetric = this.createStateManagementMetric(clientMessage);
            writeToEdgeStreamLog("Received a state management metric:     " + stateManagementMetric.toTuple());
            return stateManagementMetric;

        } else {
            throw new EdgeStreamMetricsMessageException("Found a message that is not a metric in the metrics queue, will ignore, investigate", new Throwable());
        }
    }

    private TrafficMetric createTrafficMetric(ClientMessage clientMessage) {

        String timeStamp = clientMessage.getStringProperty("timeStamp");
        String taskManagerID = clientMessage.getStringProperty("taskManagerID");
        String parentTaskManagerID = clientMessage.getStringProperty("parentTaskManagerID");
        String source = clientMessage.getStringProperty("source");
        String destination = clientMessage.getStringProperty("destination");
        String bytesSent = clientMessage.getStringProperty("bytesSent");

        TrafficMetric trafficMetric = new TrafficMetric(timeStamp, taskManagerID, parentTaskManagerID, source, destination, Long.parseLong(bytesSent));

        this.cloudMetricsDBWriter.putTrafficMetric(trafficMetric);

        return trafficMetric;
    }


    private DockerSystemMetric createDockerSystemMetric(ClientMessage clientMessage) {

        String timeStamp = clientMessage.getStringProperty("timeStamp");
        String taskManagerID = clientMessage.getStringProperty("taskManagerID");
        String operator_id = clientMessage.getStringProperty("operator_id");
        String container_id = clientMessage.getStringProperty("container_id");
        String container_name = clientMessage.getStringProperty("container_name");
        String container_cpu_utilization_percentage = clientMessage.getStringProperty("container_cpu_utilization");
        String container_cpu_core_count = clientMessage.getStringProperty("container_allocated_cpu_core_count");
        String container_memory_usage = clientMessage.getStringProperty("container_memory_usage");
        String container_memory_limit = clientMessage.getStringProperty("container_memory_limit");

        DockerSystemMetric dockerSystemMetric = new DockerSystemMetric(timeStamp, taskManagerID, operator_id, container_id
                , container_name, container_cpu_utilization_percentage, container_cpu_core_count, container_memory_usage, container_memory_limit);

        this.cloudMetricsDBWriter.putDockerSystemMetric(dockerSystemMetric);

        return dockerSystemMetric;

    }

    private ApplicationMetric createApplicationMetric(ClientMessage clientMessage) {

        String timeStamp = clientMessage.getStringProperty("timeStamp");
        String node_id = clientMessage.getStringProperty("node_id");
        String avg_latency_in_ms = clientMessage.getStringProperty("latency");

        String min_latency_in_ms = clientMessage.getStringProperty("min_latency");
        String max_latency_in_ms = clientMessage.getStringProperty("max_latency");
        String median_latency_in_ms = clientMessage.getStringProperty("median_latency");
        String std_latency_in_ms = clientMessage.getStringProperty("std_latency");

        String operator_id = clientMessage.getStringProperty("operator_id");
        String sequence_id = clientMessage.getStringProperty("sequence_id");
        String throughput = clientMessage.getStringProperty("throughput");
        String topology_id = clientMessage.getStringProperty("topology_id");
        String input_rate = clientMessage.getStringProperty("input_rate");
        String input_msg_size = clientMessage.getStringProperty("input_msg_size");
        String output_msg_size = clientMessage.getStringProperty("output_msg_size");
        String processing_time_ns = clientMessage.getStringProperty("processing_time");
        String batch_size = clientMessage.getStringProperty("batch_size");
        String buffer_consumer_size = clientMessage.getStringProperty("buffer_consumer_size");
        String buffer_producer_size = clientMessage.getStringProperty("buffer_producer_size");
        ArrayList<String> origin_list = SerializationUtils.deserialize((byte[]) clientMessage.getObjectProperty("tupleOriginID"));

        ApplicationMetric applicationMetric = new ApplicationMetric(timeStamp, node_id, avg_latency_in_ms, min_latency_in_ms, max_latency_in_ms, std_latency_in_ms, median_latency_in_ms, operator_id
                , sequence_id, throughput, topology_id, input_rate, input_msg_size, output_msg_size, processing_time_ns, batch_size
                , buffer_consumer_size, buffer_producer_size, origin_list);

        this.cloudMetricsDBWriter.putApplicationMetric(applicationMetric);

        if (this.cloudMetricsPathstoreDBWritter != null) {
            this.cloudMetricsPathstoreDBWritter.insertApplicationMetric(applicationMetric);
        }

        return applicationMetric;

    }

    private HardwareSystemMetric createHardwareSystemMetric(ClientMessage clientMessage) {

        String timeStamp = clientMessage.getStringProperty("timeStamp");
        String taskManagerID = clientMessage.getStringProperty("taskManagerID");
        String cpu_model = clientMessage.getStringProperty("host_cpu_model");
        String cpu_number_of_cores = clientMessage.getStringProperty("host_cpu_number_of_cores");
        String MemAvailable = clientMessage.getStringProperty("MemAvailable");
        String MemTotal = clientMessage.getStringProperty("MemTotal");
        String MemFree = clientMessage.getStringProperty("MemFree");
        String platform = clientMessage.getStringProperty("platform");
        String hostName = clientMessage.getStringProperty("hostName");

        HardwareSystemMetric hardwareSystemMetric = new HardwareSystemMetric(timeStamp, taskManagerID, cpu_model
                , cpu_number_of_cores, MemAvailable, MemTotal, MemFree, platform, hostName);

        this.cloudMetricsDBWriter.putHardwareSystemMetric(hardwareSystemMetric);

        return hardwareSystemMetric;

    }

    private NetworkSystemMetric createNetworkSystemMetric(ClientMessage clientMessage) {

        String timeStamp = clientMessage.getStringProperty("timeStamp");
        String taskManagerID = clientMessage.getStringProperty("taskManagerID");
        String parentTaskManagerID = clientMessage.getStringProperty("parentTaskManagerID");
        String parentTaskManagerIP = clientMessage.getStringProperty("parentTaskManagerIP");
        String linkID = clientMessage.getStringProperty("linkID");
        String available_bandwidth_mbps = clientMessage.getStringProperty("available_bandwidth_mbps");
        String average_network_latency = clientMessage.getStringProperty("average_network_latency");


        NetworkSystemMetric networkSystemMetric = new NetworkSystemMetric(timeStamp, taskManagerID, parentTaskManagerID
                , parentTaskManagerIP, linkID, available_bandwidth_mbps, average_network_latency);

        this.cloudMetricsDBWriter.putNetworkSystemMetric(networkSystemMetric);

        return networkSystemMetric;

    }

    private ApplicationEdgeMetric createApplicationEdgeMetric(ClientMessage clientMessage) {

        String timeStamp = clientMessage.getStringProperty("timeStamp");
        String sequence_id = clientMessage.getStringProperty("sequence_id");
        String topology_id = clientMessage.getStringProperty("topology_id");
        String node_id = clientMessage.getStringProperty("node_id");
        String avg_transferring_time = clientMessage.getStringProperty("transferring_time");

        String min_transferring_time = clientMessage.getStringProperty("min_transferring_time");
        String max_transferring_time = clientMessage.getStringProperty("max_transferring_time");
        String std_transferring_time = clientMessage.getStringProperty("std_transferring_time");
        String media_transferring_time = clientMessage.getStringProperty("median_transferring_time");

        String transferring_size = clientMessage.getStringProperty("transferring_size");
        String input_rate = clientMessage.getStringProperty("input_rate");
        String operator_id = clientMessage.getStringProperty("operator_id");
        String previous_operator_id = clientMessage.getStringProperty("previous_operator_id");


        ApplicationEdgeMetric applicationEdgeMetric = new ApplicationEdgeMetric(timeStamp, sequence_id, topology_id, node_id
                , avg_transferring_time, min_transferring_time, max_transferring_time, std_transferring_time, media_transferring_time, transferring_size, input_rate, operator_id, previous_operator_id);


        this.cloudMetricsDBWriter.putApplicationEdgeMetric(applicationEdgeMetric);

        return applicationEdgeMetric;

    }

    private ApplicationStateMetric createApplicationStateMetric(ClientMessage clientMessage) {

        String timeStamp = clientMessage.getStringProperty("timeStamp");
        String sequence_id = clientMessage.getStringProperty("sequence_id");
        String message_type = clientMessage.getStringProperty("message_type");
        String topology_id = clientMessage.getStringProperty("topology_id");
        String node_id = clientMessage.getStringProperty("node_id");
        String operator_id = clientMessage.getStringProperty("operator_id");
        String tupleType = clientMessage.getStringProperty("tupleType");
        String stateType = clientMessage.getStringProperty("state_type");
        String key = clientMessage.getStringProperty("keys");
        String sizeBytes = clientMessage.getStringProperty("bytes");
        String items = clientMessage.getStringProperty("number_of_items");
        String windowingTime = clientMessage.getStringProperty("window_time");

        ApplicationStateMetric applicationStateMetric = new ApplicationStateMetric(timeStamp, sequence_id, topology_id, node_id, operator_id, stateType, key, sizeBytes, items, windowingTime);


//        this.cloudMetricsDBWriter.putApplicationStateMetric(applicationStateMetric);

        return applicationStateMetric;


    }

    private ReconfigurationMetric createReconfigurationMetric(ClientMessage clientMessage) {

        String timeStamp = clientMessage.getStringProperty("timeStamp");
        String sequence_id = clientMessage.getStringProperty("sequence_id");
        String message_type = clientMessage.getStringProperty("message_type");
        String topology_id = clientMessage.getStringProperty("topology_id");
        String node_id = clientMessage.getStringProperty("node_id");
        String operator_id = clientMessage.getStringProperty("operator_id");
        String tupleType = clientMessage.getStringProperty("tupleType");
        String operation = clientMessage.getStringProperty("operation_type");
        String sourceCreationTimestamp = clientMessage.getStringProperty("creation_timestamp");

        ReconfigurationMetric reconfigurationMetric = new ReconfigurationMetric(timeStamp, sequence_id, topology_id, node_id, operator_id, operation, sourceCreationTimestamp);

//        this.cloudMetricsDBWriter.putReconfigurationMetric(reconfigurationMetric);

        return reconfigurationMetric;


    }

    private StateManagementMetric createStateManagementMetric(ClientMessage clientMessage) {

        String timeStamp = clientMessage.getStringProperty("timeStamp");
        String sequence_id = clientMessage.getStringProperty("sequence_id");
        String message_type = clientMessage.getStringProperty("message_type");
        String topology_id = clientMessage.getStringProperty("topology_id");
        String node_id = clientMessage.getStringProperty("node_id");
        String operator_id = clientMessage.getStringProperty("operator_id");
        String tupleType = clientMessage.getStringProperty("tupleType");
        String operationType = clientMessage.getStringProperty("operation_type");
        String keys = clientMessage.getStringProperty("keys");
        String sizeBytes = clientMessage.getStringProperty("bytes");
        String items = clientMessage.getStringProperty("number_of_items");
        String duration = clientMessage.getStringProperty("duration_time");

        StateManagementMetric stateManagementMetric = new StateManagementMetric(timeStamp, sequence_id, topology_id, node_id, operator_id, operationType, keys, sizeBytes, items, duration);

//        this.cloudMetricsDBWriter.putStateManagementMetric(stateManagementMetric);

        return stateManagementMetric;


    }
}
