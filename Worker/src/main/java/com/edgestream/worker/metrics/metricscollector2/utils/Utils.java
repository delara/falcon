package com.edgestream.worker.metrics.metricscollector2.utils;

import java.util.ArrayList;

public class Utils {
    public static double sd(ArrayList<Long> table, double mean) {
        // Step 1:
//        double mean = mean(table)/1000000000;
        double temp = 0d;

        for (int i = 0; i < table.size(); i++) {
            double val = table.get(i).doubleValue();

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
}
