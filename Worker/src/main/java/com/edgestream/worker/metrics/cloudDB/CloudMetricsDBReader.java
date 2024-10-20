package com.edgestream.worker.metrics.cloudDB;

import com.edgestream.worker.metrics.common.StateCollection;
import com.edgestream.worker.metrics.model.OperatorStateMetric;
import com.edgestream.worker.runtime.network.DataCenter;
import com.edgestream.worker.runtime.reconfiguration.data.*;
import org.apache.commons.lang3.SerializationUtils;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

public class CloudMetricsDBReader {

    MetricsDBService metricsDBService;

    public CloudMetricsDBReader(MetricsDBService metricsDBService) {

        this.metricsDBService = metricsDBService;

    }


    /************************************************************************************************************
     *
     *
     *                                      Compute Resource queries
     *
     *
     ***********************************************************************************************************/


    /**
     * This function gets the {@link ComputingResource} statistics for all the task managers running in the datacenters
     * TODO: will need to adjust this function to handle multiple workers in a data center
     * @param rootDataCenter
     * @return
     */
    public ArrayList<ComputingResource> getAllComputingResourceStats(DataCenter rootDataCenter) {
        //1. Query the database for all HardwareMetrics
        ArrayList<ComputingResource> computingResources = new ArrayList<>();

        //2. traverse the tree from the root and get all the task manager IDs
        ArrayList<String> taskManagers = rootDataCenter.getTaskManagerList(rootDataCenter,new ArrayList<>());

        for(String taskManager: taskManagers){

            computingResources.add(getComputingResourceStats(taskManager));

            //System.out.println("halt");
        }

        return computingResources;
    }


    /**
     * This function gets the stats for a single TaskManager
     * TODO: need to change this to maybe get the stats for many worker machines in a datacenter. For noew we only have one machine per datacenter
     * @param TaskManagerID
     * @return
     */
    private ComputingResource getComputingResourceStats(String TaskManagerID) {

        ComputingResource computingResource = null;
        Statement stmt = null;
        try {
            stmt = metricsDBService.getDatabaseConnection().createStatement();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }

        try {


            stmt.setMaxRows(1);

            String getHardwareMetricsQuery = "SELECT timeStamp" +
                    ", taskManagerID" +
                    ", host_cpu_model" +
                    ", host_cpu_number_of_cores" +
                    ", MemAvailable" +
                    ", MemTotal" +
                    ", MemFree" +
                    ", platform" +
                    ", hostName" +
                    " FROM HardwareSystemMetrics WHERE taskManagerID =" + "'" + TaskManagerID + "'" + " ORDER BY timeStamp DESC ";


            ResultSet rs = stmt.executeQuery(getHardwareMetricsQuery);

            while (rs.next()) {
                //System.out.println("-----------------------------------------------------------------------");
                String[] hardwareMetricsColumns = new String[5];
                hardwareMetricsColumns[0] = rs.getString("taskManagerID");
                hardwareMetricsColumns[1] = rs.getString("host_cpu_number_of_cores");
                hardwareMetricsColumns[2] = rs.getString("host_cpu_model");
                hardwareMetricsColumns[3] = "2.0"; //TODO: need to get cpu MHz from /proc/cpuinfo
                hardwareMetricsColumns[4] = rs.getString("MemAvailable");


                computingResource = new ComputingResource(hardwareMetricsColumns);

            }

        }catch (SQLException sqlException){
            sqlException.printStackTrace();
        }

        return computingResource;

    }


    /**
     * This function gets all computing stats for the entire network. Use for testing purposes only
     * @return
     * @throws SQLException
     */
    public ArrayList<ComputingResource> getComputingResourceStats() throws SQLException {

        //1. Query the database for all HardwareMetrics
        ArrayList<ComputingResource> computingResources = new ArrayList<>();

        Statement stmt = null;
        try {
            stmt = metricsDBService.getDatabaseConnection().createStatement();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }

        String getHardwareMetricsQuery = "SELECT timeStamp" +
                ", taskManagerID" +
                ", host_cpu_model" +
                ", host_cpu_number_of_cores" +
                ", MemAvailable" +
                ", MemTotal" +
                ", MemFree" +
                ", platform" +
                ", hostName" +
                " FROM HardwareSystemMetrics ORDER BY timeStamp DESC ";

        ResultSet rs = stmt.executeQuery(getHardwareMetricsQuery);

        while(rs.next()) {
            System.out.println("----------------------------------------");
            String[] hardwareMetricsColumns = new String[5];
            //resource_id; number_of_cores; processor_name; processor_frequency; available_memory
            hardwareMetricsColumns[0] = rs.getString("taskManagerID");
            hardwareMetricsColumns[1] = rs.getString("host_cpu_number_of_cores");
            hardwareMetricsColumns[2] = rs.getString("host_cpu_model");
            hardwareMetricsColumns[3] = "2.0"; //TODO: need to get cpu MHz from /proc/cpuinfo
            hardwareMetricsColumns[4] = rs.getString("MemAvailable");


            ComputingResource computingResource = new ComputingResource(hardwareMetricsColumns);


            computingResources.add(computingResource);
            for(String s : hardwareMetricsColumns){

                System.out.print(s + "|");
            }
            System.out.println();
            System.out.println("----------------------------------------");

        }

        return computingResources;
    }



    /************************************************************************************************************
     *
     *
     *                                          Network Link queries
     *
     *
     ***********************************************************************************************************/


    public  ArrayList<NetworkLink> getAllNetworkLinkStats(DataCenter rootDataCenter) {
        //1. Query the database for all HardwareMetrics
        ArrayList<NetworkLink> networkLinks = new ArrayList<>();

        //2. traverse the tree from the root and get all the task manager IDs
        ArrayList<String> taskManagers = rootDataCenter.getTaskManagerList(rootDataCenter,new ArrayList<>());

        for(String taskManager: taskManagers){
            networkLinks.add(getNetworkLinkStats(taskManager));
        }

        return networkLinks;
    }


    private NetworkLink getNetworkLinkStats(String TaskManagerID) {

        NetworkLink networkLink = null;
        Statement stmt = null;
        try {
            stmt = metricsDBService.getDatabaseConnection().createStatement();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }

        try {


            stmt.setMaxRows(1);

            String getNetworkMetricsQuery = "SELECT timeStamp" +
                    ", taskManagerID" +
                    ", parentTaskManagerID" +
                    ", available_bandwidth_mbps" +
                    ", average_network_latency" +
                    " FROM NetworkSystemMetrics WHERE taskManagerID =" + "'" + TaskManagerID + "'" + " ORDER BY timeStamp DESC ";


            ResultSet rs = stmt.executeQuery(getNetworkMetricsQuery);

            while (rs.next()) {
                //System.out.println("-----------------------------------------------------------------------");
                String[] networkMetricsColumns = new String[4];
                networkMetricsColumns[0] = rs.getString("taskManagerID");
                networkMetricsColumns[1] = rs.getString("parentTaskManagerID");
                networkMetricsColumns[2] = rs.getString("available_bandwidth_mbps");
                networkMetricsColumns[3] = rs.getString("average_network_latency");

                networkLink = new NetworkLink(networkMetricsColumns);
            }
        }catch (SQLException sqlException){
            sqlException.printStackTrace();
        }

        return networkLink;
    }


    /**********************************************************************************************************
     *
     *
     *                          Current operator deployment information queries
     *
     *
     ***********************************************************************************************************/


    public ArrayList<OperatorMap> getAllOperatorMaps(DataCenter rootDataCenter) {
        //1. Query the database for all HardwareMetrics
        ArrayList<OperatorMap> operatorMaps = new ArrayList<>();

        //2. traverse the tree from the root and get all the task manager IDs
        ArrayList<String> taskManagers = rootDataCenter.getTaskManagerList(rootDataCenter,new ArrayList<>());

        for(String taskManager: taskManagers){

            operatorMaps.addAll(getOperatorMapsPerTaskManager(taskManager));

            //System.out.println("halt");
        }

        return operatorMaps;
    }






    private  ArrayList<OperatorMap> getOperatorMapsPerTaskManager(String TaskManagerID){

        ArrayList<OperatorMap> operatorMap  = new ArrayList<>();
        Statement stmt = null;
        try {
            stmt = metricsDBService.getDatabaseConnection().createStatement();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }

        try {

            String getApplicationEdgeMetricsQuery = "SELECT DISTINCT " +
                    "node_id" +
                    ", operator_id" +
                    " FROM ApplicationEdgeMetrics WHERE node_id =" + "'" + TaskManagerID + "'";


            ResultSet rs = stmt.executeQuery(getApplicationEdgeMetricsQuery);

            while (rs.next()) {
                //System.out.println("-----------------------------------------------------------------------");
                String[] operatorMapColumns = new String[2];
                //resource_id; number_of_cores; processor_name; processor_frequency; available_memory
                operatorMapColumns[0] = rs.getString("node_id");
                operatorMapColumns[1] = rs.getString("operator_id");

                operatorMap.add(new OperatorMap(operatorMapColumns));

            }

        }catch (SQLException sqlException){
            sqlException.printStackTrace();
        }

        return operatorMap;

    }



    /**********************************************************************************************************
     *
     *
     *                          Operator Stats Queries
     *
     *
     ***********************************************************************************************************/



    public ArrayList<OperatorStatistics> getAllOperatorStatistics(ArrayList<OperatorMap> operatorMaps) {
        //1. Query the database for all HardwareMetrics
        ArrayList<OperatorStatistics> operatorStatistics = new ArrayList<>();

        //2. Since we have the live operator mapping we can query based on the resource ID and Operator ID

        for(OperatorMap operatorMap: operatorMaps){

            operatorStatistics.addAll(getPerOperatorStatistics(operatorMap));

            //System.out.println("halt");
        }

        return operatorStatistics;
    }






    private  ArrayList<OperatorStatistics> getPerOperatorStatistics(OperatorMap operatorMap){

        ArrayList<OperatorStatistics> operatorStatistics  = new ArrayList<>();

        String TaskManagerID = operatorMap.getResourceId();
        String OperatorID = operatorMap.getOperatorId();

        Statement stmt = null;
        try {
            stmt = metricsDBService.getDatabaseConnection().createStatement();
            stmt.setMaxRows(1); // we only want the latest result
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }

        String[] operatorStatisticsColumns = new String[11];



        /**Get App Stats**/


        try {

            String operatorAPPStatsQuery = "SELECT " +
                    "  operator_id" +
                    ", node_id" +
                    ", input_rate" +
                    ", throughput" +
                    ", input_msg_size" +
                    ", output_msg_size" +
                    ", processing_time_ns" +
                    ", origin_list" +
                    " FROM ApplicationMetrics" +
                    " WHERE node_id =" + "'" + TaskManagerID + "'" + " AND operator_id =" + "'" + OperatorID + "'" + " ORDER BY timeStamp DESC ";

            String operatorDockerStatsQuery =  "SELECT " +
                    "  container_cpu_utilization" +
                    ", container_memory_usage" +
                    " FROM DockerSystemMetrics" +
                    " WHERE taskManagerID =" + "'" + TaskManagerID + "'" + " AND operator_id =" + "'" + OperatorID + "'" + " ORDER BY timeStamp DESC ";

            ResultSet rs1 = stmt.executeQuery(operatorAPPStatsQuery);


            while (rs1.next()) {
                //System.out.println("-----------------------------------------------------------------------");

                //resource_id; number_of_cores; processor_name; processor_frequency; available_memory
                operatorStatisticsColumns[0] = rs1.getString("operator_id");
                operatorStatisticsColumns[1] = "no replicaID";
                operatorStatisticsColumns[2] = rs1.getString("node_id");


                operatorStatisticsColumns[5] = rs1.getString("input_rate");
                operatorStatisticsColumns[6] = rs1.getString("throughput");
                operatorStatisticsColumns[7] = rs1.getString("input_msg_size");
                operatorStatisticsColumns[8] = rs1.getString("output_msg_size");
                operatorStatisticsColumns[9] = rs1.getString("processing_time_ns");
                operatorStatisticsColumns[10] = getTupleOriginListAsString(SerializationUtils.deserialize(rs1.getBytes("origin_list")));


            }

            ResultSet rs2 = stmt.executeQuery(operatorDockerStatsQuery);
            while (rs2.next()) {
                //System.out.println("-----------------------------------------------------------------------");

                //resource_id; number_of_cores; processor_name; processor_frequency; available_memory
                operatorStatisticsColumns[3] = rs2.getString("container_cpu_utilization");
                operatorStatisticsColumns[4] = rs2.getString("container_memory_usage");

            }

        }catch (SQLException sqlException){
            sqlException.printStackTrace();
        }

        operatorStatistics.add(new OperatorStatistics(operatorStatisticsColumns));

        return operatorStatistics;

    }

    public String getTupleOriginListAsString(ArrayList<String> tupleOriginList){

        StringBuffer sb = new StringBuffer();

        for (String s : tupleOriginList) {
            sb.append(s);
            sb.append("|");
        }
        return sb.toString();

    }


    /**************************************************************************************************
     *
     *
     *
     *                              State Management Queries
     *
     *
     *
     **************************************************************************************************/


    /**
     * This function gives you the most recent operator state stat metric for every operator that emits an {@link OperatorStateMetric}.
     * If there are no stateful operators that report this metric, then the ArrayList will be empty.
     * TODO: right now this function gets the debug fields key_value and key_counter. In the future we need to parse the
     *  state_list BLOB object from the derby database and return a set of partition keys for a given operator.
     *
     * @param operatorMaps
     * @return
     */
    public ArrayList<OperatorPartitionKeyStats> getAllOperatorPartitionKeyStats(ArrayList<OperatorMap> operatorMaps) {

        ArrayList<OperatorPartitionKeyStats> operatorPartitionKeyStats = new ArrayList<>();

        for(OperatorMap operatorMap: operatorMaps){

            String TaskManagerID = operatorMap.getResourceId();
            String OperatorID = operatorMap.getOperatorId();

            Statement stmt = null;
            try {
                stmt = metricsDBService.getDatabaseConnection().createStatement();
                stmt.setMaxRows(1); // we only want the latest result
            } catch (SQLException throwables) {
                throwables.printStackTrace();
            }

            /**Get Partition Key Stats**/


            try {

                String operatorPartitionKeyStatsQuery = "SELECT " +
                        "timeStamp" +
                        ", topology_id" +
                        ", state_collection" +
                        " FROM OperatorStateMetrics" +
                        " WHERE node_id =" + "'" + TaskManagerID + "'" + " AND operator_id =" + "'" + OperatorID + "'" + " ORDER BY timeStamp DESC ";



                ResultSet rs1 = stmt.executeQuery(operatorPartitionKeyStatsQuery);


                while (rs1.next()) {
                    //System.out.println("-----------------------------------------------------------------------");

                    String timestamp = rs1.getString("timestamp");
                    String topologyID = rs1.getString("topology_id");
                    StateCollection stateCollection = SerializationUtils.deserialize(rs1.getBytes("state_collection"));

                    operatorPartitionKeyStats.add(new OperatorPartitionKeyStats(TaskManagerID,topologyID ,OperatorID,timestamp,stateCollection));

                }
            }catch (SQLException sqlException){

                sqlException.printStackTrace();
            }

        }

        return operatorPartitionKeyStats;
    }






    /**************************************************************************************************
     *
     *
     *
     *                              Hardware Stats Queries
     *
     *
     *
     **************************************************************************************************/



    public void printHardwareMetrics() throws SQLException {

        Statement stmt = null;
        try {
            stmt = metricsDBService.getDatabaseConnection().createStatement();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }

        String printNetworkMetricsQuery = "SELECT timeStamp" +
                ", taskManagerID" +
                ", host_cpu_model" +
                " FROM HardwareSystemMetrics";

        ResultSet rs = stmt.executeQuery(printNetworkMetricsQuery);

        while(rs.next()) {

            System.out.print("timeStamp: " + rs.getString("timeStamp"));
            System.out.print("|taskManagerID: " + rs.getString("taskManagerID"));
            System.out.print("|host_cpu_model: " + rs.getString("host_cpu_model"));


            System.out.println(" ");
        }

    }



    /**************************************************************************************************
     *
     *
     *
     *                             Network Stats Queries
     *
     *
     *
     **************************************************************************************************/


    public void printNetworkMetrics() throws SQLException {

        Statement stmt = null;
        try {
            stmt = metricsDBService.getDatabaseConnection().createStatement();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }

        String printNetworkMetricsQuery = "SELECT timeStamp" +
                ", taskManagerID" +
                ", parentTaskManagerID" +
                ", parentTaskManagerIP" +
                ", linkID" +
                " FROM NetworkSystemMetrics";

        ResultSet rs = stmt.executeQuery(printNetworkMetricsQuery);

        while(rs.next()) {

            System.out.print("timeStamp: " + rs.getString("timeStamp"));
            System.out.print("|taskManagerID: " + rs.getString("taskManagerID"));
            System.out.print("|parentTaskManagerID: " + rs.getString("parentTaskManagerID"));
            System.out.print("|parentTaskManagerIP: " + rs.getString("parentTaskManagerIP"));
            System.out.print("|linkID: " + rs.getString("linkID"));

            System.out.println(" ");
        }

    }



}
