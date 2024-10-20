package com.edgestream.worker.metrics.cloudDB;

import com.edgestream.worker.metrics.model.*;
import org.apache.commons.lang3.SerializationUtils;

import java.sql.PreparedStatement;

public class CloudMetricsDBWriter {

    MetricsDBService metricsDBService;

    public CloudMetricsDBWriter(MetricsDBService metricsDBService) {

        this.metricsDBService = metricsDBService;

    }


    public void putApplicationMetric(ApplicationMetric applicationMetric) {

        String insertMetricSQL = "INSERT INTO ApplicationMetrics VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

        try {
            PreparedStatement pstmt = metricsDBService.getDatabaseConnection().prepareStatement(insertMetricSQL);

            pstmt.setString(1, applicationMetric.getTimeStamp());
            pstmt.setString(2, applicationMetric.getNode_id());
            pstmt.setString(3, applicationMetric.getAvgLatency_in_ms());

            pstmt.setString(4, applicationMetric.getMinLatency_in_ms());
            pstmt.setString(5, applicationMetric.getMaxLatency_in_ms());
            pstmt.setString(6, applicationMetric.getStdLatency_in_ms());
            pstmt.setString(7, applicationMetric.getMedianLatency_in_ms());

            pstmt.setString(8, applicationMetric.getOperator_id());
            pstmt.setString(9, applicationMetric.getSequence_id());
            pstmt.setString(10, applicationMetric.getThroughput());
            pstmt.setString(11, applicationMetric.getTopology_id());

            pstmt.setString(12, applicationMetric.getInput_rate());
            pstmt.setString(13, applicationMetric.getInput_msg_size());
            pstmt.setString(14, applicationMetric.getOutput_msg_size());
            pstmt.setString(15, applicationMetric.getProcessing_time_ns());

            pstmt.setString(16, applicationMetric.getBatch_size());
            pstmt.setString(17, applicationMetric.getBuffer_consumer_size());
            pstmt.setString(18, applicationMetric.getBuffer_producer_size());
            pstmt.setBytes(19, SerializationUtils.serialize(applicationMetric.getTupleOrigin()));


            pstmt.executeUpdate();

        } catch (Exception e) {

            e.printStackTrace();

        }

    }


    public void putApplicationEdgeMetric(ApplicationEdgeMetric applicationEdgeMetric) {

        String insertMetricSQL = "INSERT INTO  ApplicationEdgeMetrics VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)";

        try {
            PreparedStatement pstmt = metricsDBService.getDatabaseConnection().prepareStatement(insertMetricSQL);

            pstmt.setString(1, applicationEdgeMetric.getTimeStamp());
            pstmt.setString(2, applicationEdgeMetric.getSequence_id());
            pstmt.setString(3, applicationEdgeMetric.getTopology_id());
            pstmt.setString(4, applicationEdgeMetric.getNode_id());
            pstmt.setString(5, applicationEdgeMetric.getAvgTransferring_time());

            pstmt.setString(6, applicationEdgeMetric.getMinTransferring_time());
            pstmt.setString(7, applicationEdgeMetric.getMaxTransferring_time());
            pstmt.setString(8, applicationEdgeMetric.getStdTransferring_time());
            pstmt.setString(9, applicationEdgeMetric.getMedianTransferring_time());

            pstmt.setString(10, applicationEdgeMetric.getTransferring_size());
            pstmt.setString(11, applicationEdgeMetric.getInput_rate());
            pstmt.setString(12, applicationEdgeMetric.getOperator_id());
            pstmt.setString(13, applicationEdgeMetric.getPrevious_operator_id());

            pstmt.executeUpdate();

        } catch (Exception e) {

            e.printStackTrace();

        }

    }

    public void putDockerSystemMetric(DockerSystemMetric dockerSystemMetric) {

        String insertMetricSQL = "INSERT INTO  DockerSystemMetrics VALUES (?,?,?,?,?,?,?,?,?)";

        try {
            PreparedStatement pstmt = metricsDBService.getDatabaseConnection().prepareStatement(insertMetricSQL);

            pstmt.setString(1, dockerSystemMetric.getTimeStamp());
            pstmt.setString(2, dockerSystemMetric.getTaskManagerID());
            pstmt.setString(3, dockerSystemMetric.getOperator_ID());
            pstmt.setString(4, dockerSystemMetric.getDockerContainer_id());
            pstmt.setString(5, dockerSystemMetric.getDockerContainer_name());
            pstmt.setString(6, dockerSystemMetric.getCpu_utilization());
            pstmt.setString(7, dockerSystemMetric.getCpu_core_count());
            pstmt.setString(8, dockerSystemMetric.getMemory_usage());
            pstmt.setString(9, dockerSystemMetric.getMemory_limit());

            pstmt.executeUpdate();

        } catch (Exception e) {

            e.printStackTrace();

        }


    }

    public void putHardwareSystemMetric(HardwareSystemMetric hardwareSystemMetric) {


        if (!hardwareSystemMetric.anyFieldIsEmpty()) {
            String insertMetricSQL = "INSERT INTO HardwareSystemMetrics VALUES (?,?,?,?,?,?,?,?,?)";

            try {
                PreparedStatement pstmt = metricsDBService.getDatabaseConnection().prepareStatement(insertMetricSQL);

                pstmt.setString(1, hardwareSystemMetric.getTimeStamp());
                pstmt.setString(2, hardwareSystemMetric.getTaskManagerID());
                pstmt.setString(3, hardwareSystemMetric.getCpu_model());
                pstmt.setString(4, hardwareSystemMetric.getCpu_number_of_cores());
                pstmt.setLong(5, Long.parseLong(hardwareSystemMetric.getMemAvailable()));
                pstmt.setString(6, hardwareSystemMetric.getMemTotal());
                pstmt.setString(7, hardwareSystemMetric.getMemFree());
                pstmt.setString(8, hardwareSystemMetric.getPlatform());
                pstmt.setString(9, hardwareSystemMetric.getHostName());

                pstmt.executeUpdate();

            } catch (Exception e) {

                e.printStackTrace();

            }
        }
    }

    public void putNetworkSystemMetric(NetworkSystemMetric networkSystemMetric) {

        String insertMetricSQL = "INSERT INTO  NetworkSystemMetrics VALUES (?,?,?,?,?,?,?)";

        try {
            PreparedStatement pstmt = metricsDBService.getDatabaseConnection().prepareStatement(insertMetricSQL);

            pstmt.setString(1, networkSystemMetric.getTimeStamp());
            pstmt.setString(2, networkSystemMetric.getTaskManagerID());
            pstmt.setString(3, networkSystemMetric.getParentTaskManagerID());
            pstmt.setString(4, networkSystemMetric.getParentTaskManagerIP());
            pstmt.setString(5, networkSystemMetric.getLinkID());
            pstmt.setString(6, networkSystemMetric.getAvailable_bandwidth_mbps());
            pstmt.setString(7, networkSystemMetric.getAverage_network_latency());


            pstmt.executeUpdate();

        } catch (Exception e) {

            e.printStackTrace();

        }

    }

    public void putTrafficMetric(TrafficMetric trafficMetric) {

        String insertMetricSQL = "INSERT INTO TrafficMetrics VALUES (?,?,?,?,?,?)";

        try {
            PreparedStatement pstmt = metricsDBService.getDatabaseConnection().prepareStatement(insertMetricSQL);

            pstmt.setString(1, trafficMetric.getTimeStamp());
            pstmt.setString(2, trafficMetric.getLocalTaskManagerID());
            pstmt.setString(3, trafficMetric.getLocalTaskManagerIP());
            pstmt.setString(4, trafficMetric.getParentTaskManagerID());
            pstmt.setString(5, trafficMetric.getParentTaskManagerIP());
            pstmt.setLong(6, trafficMetric.getBytesSent());

            pstmt.executeUpdate();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    public void putApplicationStateMetric(ApplicationStateMetric applicationStateMetric) {
        String insertMetricSQL = "INSERT INTO ApplicationStateMetrics VALUES (?,?,?,?,?,?,?,?,?,?)";

        try {
            PreparedStatement pstmt = metricsDBService.getDatabaseConnection().prepareStatement(insertMetricSQL);


            pstmt.setString(1, applicationStateMetric.getSequence_id());
            pstmt.setString(2, applicationStateMetric.getTimeStamp());
            pstmt.setString(3, applicationStateMetric.getTopology_id());
            pstmt.setString(4, applicationStateMetric.getNode_id());
            pstmt.setString(5, applicationStateMetric.getOperator_id());
            pstmt.setString(6, applicationStateMetric.getStateType());
            pstmt.setString(7, applicationStateMetric.getKey());
            pstmt.setString(8, applicationStateMetric.getSizeBytes());
            pstmt.setString(9, applicationStateMetric.getItems());
            pstmt.setString(10, applicationStateMetric.getWindowingTime());

            pstmt.executeUpdate();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void putReconfigurationMetric(ReconfigurationMetric reconfigurationMetric) {
        String insertMetricSQL = "INSERT INTO ReconfigurationMetrics VALUES (?,?,?,?,?,?,?)";

        try {
            PreparedStatement pstmt = metricsDBService.getDatabaseConnection().prepareStatement(insertMetricSQL);


            pstmt.setString(1, reconfigurationMetric.getSequence_id());
            pstmt.setString(2, reconfigurationMetric.getTimeStamp());
            pstmt.setString(3, reconfigurationMetric.getTopology_id());
            pstmt.setString(4, reconfigurationMetric.getNode_id());
            pstmt.setString(5, reconfigurationMetric.getOperator_id());
            pstmt.setString(6, reconfigurationMetric.getOperation());
            pstmt.setString(7, reconfigurationMetric.getSourceCreationTimestamp());

            pstmt.executeUpdate();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void putStateManagementMetric(StateManagementMetric stateManagementMetric) {
        String insertMetricSQL = "INSERT INTO StateManagementMetrics VALUES (?,?,?,?,?,?,?,?,?,?)";

        try {
            PreparedStatement pstmt = metricsDBService.getDatabaseConnection().prepareStatement(insertMetricSQL);


            pstmt.setString(1, stateManagementMetric.getSequence_id());
            pstmt.setString(2, stateManagementMetric.getTimeStamp());
            pstmt.setString(3, stateManagementMetric.getTopology_id());
            pstmt.setString(4, stateManagementMetric.getNode_id());
            pstmt.setString(5, stateManagementMetric.getOperator_id());
            pstmt.setString(6, stateManagementMetric.getOperationType());
            pstmt.setString(7, stateManagementMetric.getKeys());
            pstmt.setString(8, stateManagementMetric.getSizeBytes());
            pstmt.setString(9, stateManagementMetric.getItems());
            pstmt.setString(10, stateManagementMetric.getDuration());

            pstmt.executeUpdate();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
