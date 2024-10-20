package com.edgestream.worker.storage;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

public class GeneratePartitionKeySet {

    public static void generatePartitionKeyFile(HashMap<String, Integer> keyHitRate) {
        File newFile = new File("KeysDump.csv");
        try {
            FileWriter writer = new FileWriter(newFile);
            Integer totalSize = keyHitRate.size();
            Integer count = 0;
            for (String key: keyHitRate.keySet()) {
                if (count == totalSize-1) {
                    writer.write(key);
                    break;
                } else {
                    writer.write(key + ",");
                }
                count++;
            }
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

//    public static void generatePartitionKeyFile(HashMap<String, Integer> keyHitRate) {
//        File newFile = new File("PartitionKeysSet.java");
//        Integer totalSize = keyHitRate.size();
//        Integer count = 0;
//        String keysString = "";
//        for (String key: keyHitRate.keySet()) {
//            if (count == totalSize-1) {
//                    keysString += key;
//                } else {
//                    keysString += key + ",";
//                }
//                count++;
//            }
//        try {
//            FileWriter writer = new FileWriter(newFile);
//            writer.write("package com.edgestream.worker.storage;" + "\n");
//            writer.write("public class PartitionKeysSet {" + "\n");
//            writer.write("static String sensorIds = \"" + keysString + "\";");
//            writer.write("public static String[] getSensorIds() {\n" +
//                    "        return sensorIds.split(\",\");\n" +
//                    "    } }");
//            writer.close();
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//    }

    public static void main(String[] args) {
        File file = new File("city-senML-data/SYS-inputcsv-predict-10spouts200mps-480sec-file1_short_sync.csv");
        String fileContent = "";
        ArrayList<String> partitionKeyList = new ArrayList<String>();
        HashMap<String, Integer> keyHitRate = new HashMap<>();
        int count = 0;
        try {
            Scanner sc = new Scanner(file);
            while (sc.hasNextLine()) {
                String lineContent = sc.nextLine();
                String partitionKeyValueString = lineContent.split(",")[3];
                String partitionKeyString = partitionKeyValueString.split(":")[1].replaceAll("\"", "").replaceAll("}", "");
                //if (partitionKeyString.equals(SpecificPartitionKey)) {count++;}
                if (!keyHitRate.containsKey(partitionKeyString)) {
                    //partitionKeyList.add(partitionKeyString);
                    keyHitRate.put(partitionKeyString, 1);
                } else {
                    count = keyHitRate.get(partitionKeyString);
                    keyHitRate.put(partitionKeyString, count+1);
                }
            }
        } catch (IOException e) {
            // do something
            e.printStackTrace();
        }
        generatePartitionKeyFile(keyHitRate);
    }
}
