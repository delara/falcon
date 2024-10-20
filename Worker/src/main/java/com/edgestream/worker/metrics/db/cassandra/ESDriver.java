package com.edgestream.worker.metrics.db.cassandra;

import com.edgestream.worker.metrics.model.ApplicationMetric;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;

public class ESDriver {
    public static void main(String[] args) {
        MetricsCassandraDBService metricsCassandraDBService = new CloudMetricsCassandraDB();
        CloudMetricsCassandraDBWritter cassandraDBWritter = new CloudMetricsCassandraDBWritter(metricsCassandraDBService);
        insertDummyData(cassandraDBWritter);
        metricsCassandraDBService.getDatabaseSession().close();
    }

    private static void insertDummyData(CloudMetricsCassandraDBWritter cassandraDBWritter) {
        try{
            FileInputStream fstream = new FileInputStream("C:\\Users\\jason\\IdeaProjects\\edgestream\\Worker\\src\\main\\java\\com\\edgestream\\worker\\metrics\\db\\standlone\\sample.log");
            BufferedReader br = new BufferedReader(new InputStreamReader(fstream));
            String strLine;
            /* read log line by line */
            int counter = 0;
            while ((strLine = br.readLine()) != null) {
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
                            new ArrayList<String>(Arrays.asList(parsedFields[18]))
                    );
                    cassandraDBWritter.putApplicationMetric(applicationMetric);
                    counter++;
                    if (counter == 100) {
                        break;
                    }
                }
            }
            fstream.close();
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
        }
    }
}
