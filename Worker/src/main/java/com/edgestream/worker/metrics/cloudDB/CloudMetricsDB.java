package com.edgestream.worker.metrics.cloudDB;

import org.apache.derby.jdbc.BasicEmbeddedDataSource40;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class CloudMetricsDB implements MetricsDBService {

    private final Connection conn;

    public CloudMetricsDB() throws Throwable {

        DataSource ds = makeDataSource("metricsDB", true);
        this.conn = ds.getConnection();

        //Create Metrics tables
        createApplicationMetricsSchema();
        createApplicationEdgeMetricsSchema();
        createDockerSystemMetricsSchema();
        createHardwareSystemMetricsSchema();
        createNetworkSystemMetricsSchema();
        createTrafficMetricsSchema();
        createApplicationStateSchema();
        createReconfigurationSchema();
        createStateManagementSchema();

    }


    public Connection getDatabaseConnection() {
        return this.conn;
    }


    private DataSource makeDataSource(String dbname, boolean create) {
        BasicEmbeddedDataSource40 ds = new BasicEmbeddedDataSource40();
        ds.setDatabaseName(dbname);

        if (create) {
            ds.setCreateDatabase("create");
        }
        return ds;
    }


    private void createApplicationMetricsSchema() {

        Statement stmt = null;
        try {
            stmt = this.conn.createStatement();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }

        try {
            String dropQuery = "DROP TABLE ApplicationMetrics";
            stmt.execute(dropQuery);
        } catch (Exception e) {
            System.out.println("Table does not exist");
        }

        String query1 = "CREATE TABLE ApplicationMetrics("
                + "timeStamp VARCHAR(255),"
                + "node_id VARCHAR(255),"
                + "avg_latency_in_ms VARCHAR(255),"
                + "min_latency_in_ms VARCHAR(255),"
                + "max_latency_in_ms VARCHAR(255),"
                + "std_latency_in_ms VARCHAR(255),"
                + "median_latency_in_ms VARCHAR(255),"
                + "operator_id VARCHAR(255),"
                + "sequence_id VARCHAR(255),"
                + "throughput VARCHAR(255),"
                + "topology_id VARCHAR(255),"

                + "input_rate VARCHAR(255),"
                + "input_msg_size VARCHAR(255),"
                + "output_msg_size VARCHAR(255),"
                + "processing_time_ns VARCHAR(255),"

                + "batch_size VARCHAR(255),"
                + "buffer_consumer_size VARCHAR(255),"
                + "buffer_producer_size VARCHAR(255),"
                + "origin_list BLOB" +
                ")";


        try {
            stmt.execute(query1);
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }

    }


    private void createApplicationEdgeMetricsSchema() {

        Statement stmt = null;
        try {
            stmt = this.conn.createStatement();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
        try {
            String dropQuery = "DROP TABLE ApplicationEdgeMetrics";
            stmt.execute(dropQuery);
        } catch (Exception e) {
            System.out.println("Table does not exist");
        }

        String query1 = "CREATE TABLE ApplicationEdgeMetrics("
                + "timeStamp VARCHAR(255),"
                + "sequence_id VARCHAR(255),"
                + "topology_id VARCHAR(255),"
                + "node_id VARCHAR(255),"
                + "avg_transferring_time_in_ms VARCHAR(255),"
                + "min_transferring_time_in_ms VARCHAR(255),"
                + "max_transferring_time_in_ms VARCHAR(255),"
                + "std_transferring_time_in_ms VARCHAR(255),"
                + "median_transferring_time_in_ms VARCHAR(255),"
                + "transferring_size VARCHAR(255),"
                + "input_rate VARCHAR(255),"
                + "operator_id VARCHAR(255),"
                + "previous_operator_id VARCHAR(255))";

        try {
            stmt.execute(query1);
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }

    }


    private void createDockerSystemMetricsSchema() {

        Statement stmt = null;
        try {
            stmt = this.conn.createStatement();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }

        try {
            String dropQuery = "DROP TABLE DockerSystemMetrics";
            stmt.execute(dropQuery);
        } catch (Exception e) {
            System.out.println("Table does not exist");
        }

        String query1 = "CREATE TABLE DockerSystemMetrics("
                + "timeStamp VARCHAR(255),"
                + "taskManagerID VARCHAR(255),"
                + "operator_id VARCHAR(255),"
                + "container_id VARCHAR(255),"
                + "container_name VARCHAR(255),"
                + "container_cpu_utilization VARCHAR(255),"
                + "container_allocated_cpu_core_count VARCHAR(255),"
                + "container_memory_usage VARCHAR(255),"
                + "container_memory_limit VARCHAR(255))";

        try {
            stmt.execute(query1);
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }

    }


    private void createHardwareSystemMetricsSchema() {

        Statement stmt = null;
        try {
            stmt = this.conn.createStatement();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }

        try {
            String dropQuery = "DROP TABLE HardwareSystemMetrics";
            stmt.execute(dropQuery);
        } catch (Exception e) {
            System.out.println("Table does not exist");
        }

        String query1 = "CREATE TABLE HardwareSystemMetrics("
                + "timeStamp VARCHAR(255),"
                + "taskManagerID VARCHAR(255),"
                + "host_cpu_model VARCHAR(255),"
                + "host_cpu_number_of_cores VARCHAR(255),"
                + "MemAvailable NUMERIC(20,0),"
                + "MemTotal VARCHAR(255),"
                + "MemFree VARCHAR(255),"
                + "platform VARCHAR(255),"
                + "hostName VARCHAR(255)" +
                ")";


        try {
            stmt.execute(query1);
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }

    }


    private void createNetworkSystemMetricsSchema() {

        Statement stmt = null;
        try {
            stmt = this.conn.createStatement();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }

        try {
            String dropQuery = "DROP TABLE NetworkSystemMetrics";
            stmt.execute(dropQuery);
        } catch (Exception e) {
            System.out.println("Table does not exist");
        }

        String query1 = "CREATE TABLE NetworkSystemMetrics("
                + "timeStamp VARCHAR(255),"
                + "taskManagerID VARCHAR(255),"
                + "parentTaskManagerID VARCHAR(255),"
                + "parentTaskManagerIP VARCHAR(255),"

                + "linkID VARCHAR(255),"
                + "available_bandwidth_mbps VARCHAR(255),"
                + "average_network_latency VARCHAR(255))";

        try {
            stmt.execute(query1);
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }

    }


    private void createTrafficMetricsSchema() {

        Statement stmt = null;
        try {
            stmt = this.conn.createStatement();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }

        try {
            String dropQuery = "DROP TABLE TrafficMetrics";
            stmt.execute(dropQuery);
        } catch (Exception e) {
            System.out.println("Table does not exist");
        }

        String query1 = "CREATE TABLE TrafficMetrics("
                + "timeStamp VARCHAR(255),"
                + "taskManagerID VARCHAR(255),"
                + "parentTaskManagerID VARCHAR(255),"
                + "source VARCHAR(255),"
                + "destination VARCHAR(255),"
                + "bytesSent BIGINT)";

        try {
            stmt.execute(query1);
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }

    }


    private void createApplicationStateSchema() {

        Statement stmt = null;
        try {
            stmt = this.conn.createStatement();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }

        try {
            String dropQuery = "DROP TABLE ApplicationStateMetrics";
            stmt.execute(dropQuery);
        } catch (Exception e) {
            System.out.println("Table does not exist");
        }

        String query1 = "CREATE TABLE ApplicationStateMetrics ("
                + "sequence_id VARCHAR(255),"
                + "timeStamp VARCHAR(255),"
                + "topology_id VARCHAR(255),"
                + "node_id VARCHAR(255),"
                + "operator_id VARCHAR(255),"
                + "state_type VARCHAR(255),"
                + "state_key VARCHAR(255),"
                + "sizeBytes VARCHAR(255),"
                + "items VARCHAR(255),"
                + "windowingTime VARCHAR(255)" +
                ")";


        try {
            stmt.execute(query1);
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }

    }


    private void createReconfigurationSchema() {

        Statement stmt = null;
        try {
            stmt = this.conn.createStatement();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }

        try {
            String dropQuery = "DROP TABLE ReconfigurationMetrics";
            stmt.execute(dropQuery);
        } catch (Exception e) {
            System.out.println("Table does not exist");
        }

        String query1 = "CREATE TABLE ReconfigurationMetrics ("
                + "sequence_id VARCHAR(255),"
                + "timeStamp VARCHAR(255),"
                + "topology_id VARCHAR(255),"
                + "node_id VARCHAR(255),"
                + "operator_id VARCHAR(255),"
                + "operation VARCHAR(255),"
                + "sourceCreationTimestamp VARCHAR(255)"
                + ")";

        try {
            stmt.execute(query1);
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }

    }


    private void createStateManagementSchema() {

        Statement stmt = null;
        try {
            stmt = this.conn.createStatement();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }

        try {
            String dropQuery = "DROP TABLE StateManagementMetrics";
            stmt.execute(dropQuery);
        } catch (Exception e) {
            System.out.println("Table does not exist");
        }

        String query1 = "CREATE TABLE StateManagementMetrics ("
                + "sequence_id VARCHAR(255),"
                + "timeStamp VARCHAR(255),"
                + "topology_id VARCHAR(255),"
                + "node_id VARCHAR(255),"
                + "operator_id VARCHAR(255),"
                + "operationType VARCHAR(255),"
                + "keys VARCHAR(255),"
                + "sizeBytes VARCHAR(255),"
                + "items VARCHAR(255),"
                + "duration VARCHAR(255)"
                + ")";
        try {
            stmt.execute(query1);
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }

    }


}
