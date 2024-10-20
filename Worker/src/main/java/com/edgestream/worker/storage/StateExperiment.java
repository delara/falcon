package com.edgestream.worker.storage;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import java.util.regex.Pattern;

public class StateExperiment {
    public static class KeyHitRateExperiment {
        static HashMap<String, Integer> runningCounter = new HashMap<>();
        static HashMap<String, Integer> finalResult = new HashMap<>();
        static boolean printFlag = false;
        public KeyHitRateExperiment() {
            initialiseFinalResult();
            new Thread(() -> {
                do {
                    try {
                        Thread.sleep(10000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    checkWithFinalResult();
                } while (true);
            }).start();
        }
        public void initialiseFinalResult() {
            HashMap<String, Integer> keyRate = KeyRateFileDump.getKeyRate();
            //System.out.println("KeyRateMap:" + keyRate.toString());
            String[] tupleTypes = {"humidity", "airquality_raw", "light", "temperature", "dust"};
            keyRate.forEach((key, value) -> {
                for (String tupleType: tupleTypes) {
                    finalResult.put(key+tupleType, value);
                }
            });
        }
        public void incrementHitCounter(String key) {
            String pattern = Pattern.quote("\\" + "n");
            key = key.split(pattern)[0];
            //System.out.println("Key: " + key);
            if (runningCounter.containsKey(key)) {
                runningCounter.put(key, runningCounter.get(key)+1);
            } else {
                // to do : fix this. It should be 1. Fix this at data source.
                runningCounter.put(key, 0);
            }
            //System.out.println("Running counter:" + runningCounter.toString());
        }
        public void checkWithFinalResult() {
            File newFile = new File(System.getProperty("user.home") + "/Key_Rate_FileDump");
            try {
                FileWriter writer = new FileWriter(newFile);
                for (String key : finalResult.keySet()) {
                    Integer finalCount = finalResult.get(key);
                    Integer currentCount = runningCounter.get(key);
                    if (finalCount != currentCount) {
                        writer.write("Tuple count for key: " + key + " doesnt match\n");
                        writer.write("For key: " + key + " Final Count: " + finalCount + " Current Count: " + currentCount + "\n");
                    } else {
                        writer.write("Tuple count for key: " + key + " matches\n");
                    }
                }
                writer.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
//            finalResult.forEach((key, finalCount) -> {
//                Integer currentCount = runningCounter.get(key);
//                if (finalCount != currentCount) {
//                    System.out.println("Tuple count for key: " + key + "doesnt match");
//                    System.out.println("For key: " + key + "Final Count: " + finalCount + "Current Count: " + currentCount);
//                } else {
//                    System.out.println("Tuple count for key: "+ key + "matches");
//                }
//            });
        }

        public void incrementHitAndCheck(String key) {
            incrementHitCounter(key);
//            if (printFlag) {
//                checkWithFinalResult();
//                printFlag = false;
//            }
        }
    }
}
