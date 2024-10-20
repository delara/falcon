package com.edgestream.worker.metrics.cloudDB.standlone;

import com.edgestream.worker.metrics.cloudDB.CloudMetricsDBWriter;
import com.edgestream.worker.metrics.cloudDB.MetricsDBService;
import com.edgestream.worker.metrics.model.*;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;

public class ElasticSearchDriver {

    public static void main(String[] args) {

        MetricsDBService metricsDBService = new MetricsDBDev();
        CloudMetricsDBWriter cloudMetricsDBWriter = new CloudMetricsDBWriter(metricsDBService);
        insertDummyData(cloudMetricsDBWriter);
//        CloudMetricsDBReader cloudMetricsDBReader = new CloudMetricsDBReader(metricsDBService);
//        writeToElasticSearch(cloudMetricsDBReader);

    }

//    private static void writeToElasticSearch(CloudMetricsDBReader cloudMetricsDBReader){
//
//        //TODO: use the cloudMetricsDBReader to get summary stats
//
//        //TODO: create a new reader to get the raw data and push it to elastic search
//
//    }


    private static void insertDummyData(CloudMetricsDBWriter cloudMetricsDBWriter){

        //TODO: use log file to create objects
//        ApplicationMetric applicationMetric = null;
//        OperatorStateMetric operatorStateMetric = null;
//        ConsistencyMetric consistencyMetric = null;
//        ApplicationEdgeMetric applicationEdgeMetric =null;
//        DockerSystemMetric dockerSystemMetric = null;
//        HardwareSystemMetric hardwareSystemMetric = null;
//        NetworkSystemMetric networkSystemMetric = null;
//        TrafficMetric trafficMetric = null;

        try{
            FileInputStream fstream = new FileInputStream("/Users/xuwang/Desktop/2021 Summer/Summer Research/project/Worker/src/main/java/com/edgestream/worker/metrics/db/standlone/sample.log");
            BufferedReader br = new BufferedReader(new InputStreamReader(fstream));
            String strLine;
            /* read log line by line */
            while ((strLine = br.readLine()) != null)   {
                /* parse strLine to obtain what you want */
                if (strLine.matches("Received an application metric:.*"))
                {
                    String[] parsedFields = strLine.split(":")[1].trim().split(";");
                    ApplicationMetric applicationMetric = new ApplicationMetric(
                            parsedFields[0],
                            parsedFields[1],
                            parsedFields[2],
                            parsedFields[3],
                            parsedFields[4],
                            parsedFields[5],
                            parsedFields[6],
                            parsedFields[7],
                            parsedFields[8],
                            parsedFields[9],
                            parsedFields[10],
                            parsedFields[11],
                            parsedFields[12],
                            parsedFields[13],
                            parsedFields[14],
                            parsedFields[15],
                            parsedFields[16],
                            parsedFields[17],
                            new ArrayList<String> (Arrays.asList(parsedFields[18]))
                            );
                    cloudMetricsDBWriter.putApplicationMetric(applicationMetric);
                }
            }
            fstream.close();
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
        }
//        cloudMetricsDBWriter.putApplicationMetric(applicationMetric);
//        cloudMetricsDBWriter.putOperatorStateMetric(operatorStateMetric);
//        cloudMetricsDBWriter.putConsistencyMetric(consistencyMetric);
//        cloudMetricsDBWriter.putApplicationEdgeMetric(applicationEdgeMetric);
//        cloudMetricsDBWriter.putDockerSystemMetric(dockerSystemMetric);
//        cloudMetricsDBWriter.putHardwareSystemMetric(hardwareSystemMetric);
//        cloudMetricsDBWriter.putNetworkSystemMetric(networkSystemMetric);
//        cloudMetricsDBWriter.putTrafficMetric(trafficMetric);
        

    }

}
