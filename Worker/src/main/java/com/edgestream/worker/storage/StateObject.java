package com.edgestream.worker.storage;

import com.edgestream.worker.config.EdgeStreamGetPropertyValues;
import com.edgestream.worker.runtime.reconfiguration.state.AtomicKey;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class StateObject {

    public static HashMap parseMap(String mapString) {
        mapString = mapString.replaceAll("\\{", "");
        mapString = mapString.replaceAll("\\}", "");
        HashMap<String, String> map = new HashMap<>();
        String[] mapValues = mapString.split(",");
        String[] mapPairs;
        for (int i = 0; i < mapValues.length; i++) {
            mapPairs = mapValues[i].split("=");
            map.put(mapPairs[0].trim(), mapPairs[1].trim());
        }
        return map;
    }

    public static HashMapObject restoreState(String OperatorId) {
        OperatorState operatorState;
        HashMapObject operatorStateObject = new HashMapObject(OperatorId);
        db.initialiseDBConnection();
        operatorState = db.readOperatorState(OperatorId);
        int i = 0;
        if (operatorState != null) {
            while (i < operatorState.partitionKeys.size()) {
                operatorStateObject.map.put(operatorState.partitionKeys.get(i), parseMap(operatorState.stateValues.get(i)));
                i++;
            }
        }
        return operatorStateObject;
    }

    public static class OperatorState {
        public ArrayList<Long> partitionKeys = new ArrayList<>();
        public ArrayList<String> stateValues = new ArrayList<>();
    }

    public static class HashMapObject {
        protected HashMap<Long, HashMap<String, String>> map = new HashMap<>();
        protected ConcurrentHashMap<Long, ConcurrentHashMap<String, String>> cmap = new ConcurrentHashMap<>();
        private boolean writeOccurred = false;
        private ArrayList<Long> changedKeys = new ArrayList<>();
        private final String OperatorID;

        private void updateState(HashMap<Long, HashMap<String, String>> map) {
            changedKeys.forEach((key) -> {
                HashMap<String, String> value = map.get(key);
                if (db.partitionKeyExists(key.toString())) {
                    db.updateState(key.toString(), value.toString());
                } else {
                    db.insertState(OperatorID, key.toString(), value.toString());
                }
            });
            changedKeys = new ArrayList<>();
        }

        private void checkStateChanges() {
            if (writeOccurred) {
                synchronized (map) {
                    //System.out.println("State Change occurred");
                    //System.out.println("Will write this to storage" + map.toString());
                    updateState(map);
                }
                writeOccurred = false;
            }
        }

        public HashMapObject(String OperatorID) {
            db.initialiseDBConnection();
            this.OperatorID = OperatorID;
            new Thread(() -> {
                do {
                    checkStateChanges();
                    try {
                        Thread.sleep(10);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                } while (true);
            }).start();
        }

        public HashMap<String, String> getKey(Long key) {
            return map.get(key);
        }

        public void putKey(Long key, HashMap<String, String> value) {
            synchronized (map) {
                map.put(key, value);
                writeOccurred = true;
                changedKeys.add(key);
            }
        }

        public boolean containsKey(Long key) {
            return map.containsKey(key);
        }

        public void removeKey(Long key) {
            HashMap<Long, HashMap<String, String>> oldMap = map;
            map.remove(key);
            writeOccurred = true;
        }
    }

    public static class HashMapObject2 {
        protected ConcurrentHashMap<String, HashMap<String, String>> cmap = new ConcurrentHashMap<>();
        private boolean writeOccurred = false;
        private CopyOnWriteArrayList<String> changedKeys = new CopyOnWriteArrayList<>();
        private final String OperatorID;

        private void updateState(ConcurrentHashMap<String, HashMap<String, String>> cmap, ArrayList<AtomicKey> routingKeys) {
            long startTime = System.nanoTime();
            if (routingKeys != null) {
                // For on-demand backup, take backup of only keys to be routed.
                System.out.println("Performing on-demand backup of state object for " + routingKeys.size() + " keys");
                for (AtomicKey routingKey : routingKeys) {
                    String key = routingKey.getValue();
                    if (changedKeys.contains(key)) {
                        HashMap<String, String> value = cmap.get(key);
                        PathstoreClient.insertState(OperatorID, key, value.toString());
                        changedKeys.remove(key);
                    }
                }
            } else {
                // For periodic backup, take backup of all keys whose values have been modified.
                System.out.println("Performing periodic backup of state object for " + changedKeys.size() + " keys");
                changedKeys.forEach((key) -> {
                    HashMap<String, String> value = cmap.get(key);
                    PathstoreClient.insertState(OperatorID, key, value.toString());
                });
                changedKeys = new CopyOnWriteArrayList<>();
            }
            long elapsedTime = System.nanoTime() - startTime;
            System.out.println("Completed backup of state object");
            System.out.println("Execution time for Backup operation: " + (float) elapsedTime / 1000000 + "ms");
        }

        public void checkStateChanges(ArrayList<AtomicKey> routingKeys) {
            if (writeOccurred) {
                updateState(cmap, routingKeys);
            }
        }

        public String findIpFromAddress(String address) {
            String IPADDRESS_PATTERN =
                    "(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)";

            Pattern pattern = Pattern.compile(IPADDRESS_PATTERN);
            Matcher matcher = pattern.matcher(address);
            if (matcher.find()) {
                return matcher.group();
            }
            return "";
        }

        public HashMapObject2(String OperatorID, String node_ip_address) {
            String node_ip = findIpFromAddress(node_ip_address);
            PathstoreClient.createPropertiesFile(node_ip);
            PathstoreClient.initialiseConnection();
            this.OperatorID = OperatorID;

            // This is the code for periodic checkpoint.

            new Thread(() -> {
                do {
                    checkStateChanges(null);
                    try {
                        Thread.sleep(500);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                } while (true);
            }).start();

        }

        public HashMap<String, String> getKey(String key) {
            return cmap.get(key);
        }

        public void putKey(String key, HashMap<String, String> value) {
            cmap.put(key, value);
            writeOccurred = true;
            if (!changedKeys.contains(key)) {
                changedKeys.add(key);
            }
        }

        public boolean containsKey(String key) {
            return cmap.containsKey(key);
        }

        public void printContent() {
            System.out.println("Printing the contents of the state object");
            for (String key : cmap.keySet()) {
                System.out.println("Key: " + key + " ;Value " + cmap.get(key));
            }
        }

        public boolean isRestoreRequired(ArrayList<AtomicKey> routingKeys) {
            boolean restoreRequired = false;
            for (AtomicKey routingKey : routingKeys) {
                if (!routingKey.getKey().equals("any")) {
                    restoreRequired = true;
                    break;
                }
            }
            return restoreRequired;
        }

        public HashMap<String, String> parseMap(String mapString) {
            mapString = mapString.replaceAll("\\{", "");
            mapString = mapString.replaceAll("\\}", "");
            HashMap<String, String> map = new HashMap<>();
            String[] mapValues = mapString.split(",");
            String[] mapPairs;
            for (int i = 0; i < mapValues.length; i++) {
                mapPairs = mapValues[i].split("=");
                map.put(mapPairs[0].trim(), mapPairs[1].trim());
            }
            return map;
        }

        public void reconsolidateStateObject(ArrayList<String> partitionKeys) {
            for (String partitionKey : partitionKeys) {
                String value = PathstoreClient.readState(partitionKey);
                cmap.put(partitionKey, parseMap(value));
            }
        }

        public void restorePartitionKeys(ArrayList<AtomicKey> routingKeys) {
            //System.out.println("Checking / Waiting for backup to be complete for source operator");
            //PathstoreClient.pollForBackupSignal();
            //System.out.println("Backup is complete for source operator");
            long startTime = System.nanoTime();
            System.out.println("Operator has been migrated. Triggering restore for operator.");
            ArrayList<String> partitionKeys = new ArrayList<>();
            for (AtomicKey routingKey : routingKeys) {
                partitionKeys.add(routingKey.getValue());
            }
            PathstoreClient.warmupPathstore(partitionKeys);
            reconsolidateStateObject(partitionKeys);
            //PathstoreClient.warmupPathstore();
            long elapsedTime = System.nanoTime() - startTime;
            System.out.println("Restore completed in: " + (double) elapsedTime / 1000000 + "ms");
        }
    }

    public static class HashMapObject3 {
        protected ConcurrentHashMap<String, ArrayList<Float>> cmap = new ConcurrentHashMap<>();
        private boolean writeOccurred = false;
        private CopyOnWriteArrayList<String> changedKeys = new CopyOnWriteArrayList<>();
        private final String OperatorID;

        private void updateState(ConcurrentHashMap<String, ArrayList<Float>> cmap, ArrayList<AtomicKey> routingKeys) {
            long startTime = System.nanoTime();
            if (routingKeys != null) {
                // For on-demand backup, take backup of only keys to be routed.
                System.out.println("Performing on-demand backup of state object for " + routingKeys.size() + " keys");
                for (AtomicKey routingKey : routingKeys) {
                    String key = routingKey.getValue();
                    if (changedKeys.contains(key)) {
                        ArrayList<Float> value = cmap.get(key);
                        PathstoreClient.insertState(OperatorID, key, value.toString());
                        changedKeys.remove(key);
                    }
                }
            } else {
                // For periodic backup, take backup of all keys whose values have been modified.
                System.out.println("Performing periodic backup of state object for " + changedKeys.size() + " keys");
                changedKeys.forEach((key) -> {
                    ArrayList<Float> value = cmap.get(key);
                    PathstoreClient.insertState(OperatorID, key, value.toString());
                });
                changedKeys = new CopyOnWriteArrayList<>();
            }
            long elapsedTime = System.nanoTime() - startTime;
            System.out.println("Completed backup of state object");
            System.out.println("Execution time for Backup operation: " + (float) elapsedTime / 1000000 + "ms");
        }

        public void checkStateChanges(ArrayList<AtomicKey> routingKeys) {
            if (writeOccurred) {
                updateState(cmap, routingKeys);
            }
        }

        public String findIpFromAddress(String address) {
            String IPADDRESS_PATTERN =
                    "(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)";

            Pattern pattern = Pattern.compile(IPADDRESS_PATTERN);
            Matcher matcher = pattern.matcher(address);
            if (matcher.find()) {
                return matcher.group();
            }
            return "";
        }

        public HashMapObject3(String OperatorID, String node_ip_address) {
            String node_ip = findIpFromAddress(node_ip_address);
            PathstoreClient.createPropertiesFile(node_ip);
            PathstoreClient.initialiseConnection();
            this.OperatorID = OperatorID;

            // This is the code for periodic checkpoint.

            new Thread(() -> {
                do {
                    checkStateChanges(null);
                    try {
                        Thread.sleep(500);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                } while (true);
            }).start();

        }

        public ArrayList<Float> getKey(String key) {
            return cmap.get(key);
        }

        public void putKey(String key, ArrayList<Float> value) {
            cmap.put(key, value);
            writeOccurred = true;
            changedKeys.add(key);
        }

        public boolean containsKey(String key) {
            return cmap.containsKey(key);
        }

        public boolean isRestoreRequired(ArrayList<AtomicKey> routingKeys) {
            boolean restoreRequired = false;
            for (AtomicKey routingKey : routingKeys) {
                if (!routingKey.getKey().equals("any")) {
                    restoreRequired = true;
                    break;
                }
            }
            return restoreRequired;
        }

        public ArrayList<Float> parseArrayList(String arrayListString) {
            arrayListString = arrayListString.replaceAll("\\[", "");
            arrayListString = arrayListString.replaceAll("\\]", "");
            ArrayList<Float> arrayList = new ArrayList<>();
            String[] arrayListValues = arrayListString.split(",");
            for (int i = 0; i < arrayListValues.length; i++) {
                arrayList.add(Float.parseFloat(arrayListValues[i].trim()));
            }
            return arrayList;
        }

        public void reconsolidateStateObject(ArrayList<String> partitionKeys) {
            for (String partitionKey : partitionKeys) {
                String value = PathstoreClient.readState(partitionKey);
                cmap.put(partitionKey, parseArrayList(value));
            }
        }

        public void restorePartitionKeys(ArrayList<AtomicKey> routingKeys) {
            //System.out.println("Checking / Waiting for backup to be complete for source operator");
            //PathstoreClient.pollForBackupSignal();
            //System.out.println("Backup is complete for source operator");
            long startTime = System.nanoTime();
            System.out.println("Operator has been migrated. Triggering restore for operator.");
            ArrayList<String> partitionKeys = new ArrayList<>();
            for (AtomicKey routingKey : routingKeys) {
                partitionKeys.add(routingKey.getValue());
            }
            PathstoreClient.warmupPathstore(partitionKeys);
            //PathstoreClient.warmupPathstore();
            reconsolidateStateObject(partitionKeys);
            long elapsedTime = System.nanoTime() - startTime;
            System.out.println("Restore completed in: " + (double) elapsedTime / 1000000 + "ms");
        }
    }

    public static class HashMapObject4 {
        protected ConcurrentHashMap<String, ArrayList<Double>> cmap = new ConcurrentHashMap<>();
        private boolean writeOccurred = false;
        private ArrayList<String> changedKeys = new ArrayList<>();
        private final String OperatorID;
        public long stateSize;
        public long changedKeysSize;

        private void updateState(ConcurrentHashMap<String, ArrayList<Double>> cmap, ArrayList<AtomicKey> routingKeys) {
            this.stateSize = 0;
            if (routingKeys != null) {
                // For on-demand backup, take backup of only keys to be routed.
                System.out.println("Performing on-demand backup of state object for " + routingKeys.size() + " keys");
                for (AtomicKey routingKey : routingKeys) {
                    String key = routingKey.getValue();
                    if (changedKeys.contains(key)) {
                        String value = cmap.get(key).toString();
                        this.stateSize += Long.valueOf(value.getBytes().length);
                        PathstoreClient.insertState(OperatorID, key, value);
                        changedKeys.remove(key);
                    }
                }
            } else {
                // For periodic backup, take backup of all keys whose values have been modified.
                ArrayList<String> changedKeysCopy = new ArrayList<>();
                changedKeysCopy.addAll(changedKeys);
                changedKeysSize = Long.valueOf(changedKeysCopy.size());
                System.out.println("Performing periodic backup of state object for " + changedKeysSize + " keys");
                changedKeysCopy.forEach((key) -> {
                    ArrayList<Double> value = (ArrayList<Double>) cmap.get(key).clone();
                    String valueString = value.toString();
                    this.stateSize += Long.valueOf(valueString.getBytes().length);
                    PathstoreClient.insertState(OperatorID, key, valueString);
                });
                changedKeys = new ArrayList<>();
            }
        }

        public boolean checkStateChanges(ArrayList<AtomicKey> routingKeys) {
            if (writeOccurred) {
                updateState(cmap, routingKeys);
                writeOccurred = false;
                return true;
            }
            return false;
        }

        public String findIpFromAddress(String address) {
            String IPADDRESS_PATTERN =
                    "(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)";

            Pattern pattern = Pattern.compile(IPADDRESS_PATTERN);
            Matcher matcher = pattern.matcher(address);
            if (matcher.find()) {
                return matcher.group();
            }
            return "";
        }

        public HashMapObject4(String OperatorID, String node_ip_address) {
            String node_ip = findIpFromAddress(node_ip_address);
            PathstoreClient.createPropertiesFile(node_ip);
            PathstoreClient.initialiseConnection();
            this.OperatorID = OperatorID;
            EdgeStreamGetPropertyValues.setCheckpointStatus("true");
        }

        public void pausePeriodicCheckpoint() {
            new Thread(() -> {
                System.out.println("Pausing periodic checkpoint");
                EdgeStreamGetPropertyValues.setCheckpointStatus("false");
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                System.out.println("Resuming periodic checkpoint");
                EdgeStreamGetPropertyValues.setCheckpointStatus("true");
            }).start();
        }

        public ArrayList<Double> getKey(String key) {
            return new ArrayList<>(cmap.get(key));
        }

        public void putKey(String key, ArrayList<Double> value) {
            cmap.put(key, value);
            writeOccurred = true;
            if (!changedKeys.contains(key)) {
                changedKeys.add(key);
            }
        }

        public boolean containsKey(String key) {
            return cmap.containsKey(key);
        }

        public ArrayList<Double> parseArrayList(String arrayListString) {
            arrayListString = arrayListString.replaceAll("\\[", "");
            arrayListString = arrayListString.replaceAll("\\]", "");
            ArrayList<Double> arrayList = new ArrayList<>();
            String[] arrayListValues = arrayListString.split(",");
            for (int i = 0; i < arrayListValues.length; i++) {
                arrayList.add(Double.parseDouble(arrayListValues[i].trim()));
            }
            return arrayList;
        }

        public void reconsolidateStateObject(ArrayList<String> partitionKeys) {
            this.stateSize = 0;
            for (String partitionKey : partitionKeys) {
                String value = PathstoreClient.readState(partitionKey);
                stateSize += Long.valueOf(value.getBytes().length);
                cmap.put(partitionKey, parseArrayList(value));
            }
        }

        public void restorePartitionKeys(ArrayList<AtomicKey> routingKeys) {
            //System.out.println("Checking / Waiting for backup to be complete for source operator");
            //PathstoreClient.pollForBackupSignal();
            //System.out.println("Backup is complete for source operator");
            //PathstoreClient.pollForBackupSignal();
            System.out.println("Operator has been migrated. Triggering restore for operator.");
            ArrayList<String> partitionKeys = new ArrayList<>();
            for (AtomicKey routingKey : routingKeys) {
                partitionKeys.add(routingKey.getValue());
            }
            PathstoreClient.warmupPathstore(partitionKeys);
            //PathstoreClient.warmupPathstore();
            reconsolidateStateObject(partitionKeys);
        }

        public String printContents() {
            String concatenatedResult = "";
            for (String key : cmap.keySet()) {
                concatenatedResult = concatenatedResult + key + ":" + cmap.get(key).toString() + ",";
            }
            return concatenatedResult;
        }
    }

    public static class HashMapObject5 {
        protected ConcurrentHashMap<String, ArrayList<String>> cmap = new ConcurrentHashMap<>();
        private HashMap<String, ArrayList<String>> cmap_replica = new HashMap<>();
        private boolean onDemandBackup = false;
        private boolean writeOccurred = false;
        private ArrayList<String> changedKeys = new ArrayList<>();
        private final String OperatorID;
        public long stateSize;
        public long changedKeysSize;
//        ArrayList<AtomicKey> routingKeys;
//        private ArrayList<String> changedKeysCopy;

        public void backupToPathstore() {
            long onDemandBackupStart = System.currentTimeMillis();
            System.out.println("Triggering on-demand backup for " + cmap_replica.keySet().size() + " keys");
            for (String key : cmap_replica.keySet()) {
                String value = cmap_replica.get(key).toString();
                PathstoreClient.insertState(OperatorID, key, value);
                stateSize += value.getBytes().length;
            }
            System.out.println("State size (in bytes): " + stateSize);
            cmap_replica.clear();
            PathstoreClient.dumpSessionFile();
            System.out.println("Duration for on-demand backup: " + (System.currentTimeMillis() - onDemandBackupStart) + "ms");
        }

        private void updateState(ConcurrentHashMap<String, ArrayList<String>> cmap, ArrayList<AtomicKey> routingKeys) {
            this.stateSize = 0;
            if (routingKeys != null) {
//                cmap_replica = cmap.clone();
                // For on-demand backup, take backup of only keys to be routed.
                System.out.println("Creating in-memory copy for " + routingKeys.size() + " keys");
                long inMemBackupStart = System.currentTimeMillis();
                for (AtomicKey routingKey : routingKeys) {
                    String key = routingKey.getValue();
                    if (changedKeys.contains(key)) {
                        ArrayList<String> value = (ArrayList<String>) cmap.get(key).clone();
                        cmap_replica.put(key, value);
                        System.out.println("Completed mem copy operation");
//                            stateSize += value.toString().getBytes().length;
//                            System.out.println("State size (in bytes): " + stateSize);
                        changedKeys.remove(key);
                    }
                }
                System.out.println("Duration for in-mem backup: " + (System.currentTimeMillis() - inMemBackupStart) + "ms");
//                this.routingKeys = routingKeys;
//                this.changedKeysCopy = (ArrayList<String>) changedKeys.clone();
//                new Thread(() -> {
//                    backupToPathstore(routingKeys, (ArrayList<String>)changedKeys.clone());
//                }).start();
            } else {
                // For periodic backup, take backup of all keys whose values have been modified.
                ArrayList<String> changedKeysCopy = new ArrayList<>();
                changedKeysCopy.addAll(changedKeys);
                changedKeysSize = Long.valueOf(changedKeysCopy.size());
                System.out.println("Performing periodic backup of state object for " + changedKeysSize + " keys");
                this.stateSize = 0;
                changedKeysCopy.forEach((key) -> {
                    ArrayList<String> value = (ArrayList<String>) cmap.get(key).clone();
                    String valueString = value.toString();
                    this.stateSize += Long.valueOf(valueString.getBytes().length);
                    PathstoreClient.insertState(OperatorID, key, valueString);
                });
                changedKeys = new ArrayList<>();
            }
        }

        public boolean checkStateChanges(ArrayList<AtomicKey> routingKeys) {
            if (writeOccurred) {
                updateState(cmap, routingKeys);
                writeOccurred = false;
                return true;
            }
            return false;
        }

        public String findIpFromAddress(String address) {
            String IPADDRESS_PATTERN =
                    "(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)";

            Pattern pattern = Pattern.compile(IPADDRESS_PATTERN);
            Matcher matcher = pattern.matcher(address);
            if (matcher.find()) {
                return matcher.group();
            }
            return "";
        }

        public HashMapObject5(String OperatorID, String node_ip_address) {
            String node_ip = findIpFromAddress(node_ip_address);
            PathstoreClient.createPropertiesFile(node_ip);
            PathstoreClient.initialiseConnection();
            this.OperatorID = OperatorID;
            EdgeStreamGetPropertyValues.setCheckpointStatus("true");
        }

        public void pausePeriodicCheckpoint() {
            new Thread(() -> {
                System.out.println("Pausing periodic checkpoint");
                EdgeStreamGetPropertyValues.setCheckpointStatus("false");
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                System.out.println("Resuming periodic checkpoint");
                EdgeStreamGetPropertyValues.setCheckpointStatus("true");
            }).start();
        }

        public ArrayList<String> getKey(String key) {
            return new ArrayList<>(cmap.get(key));
        }

        public void putKey(String key, ArrayList<String> value) {
            cmap.put(key, value);
            writeOccurred = true;
            if (!changedKeys.contains(key)) {
                changedKeys.add(key);
            }
        }

        public boolean containsKey(String key) {
            return cmap.containsKey(key);
        }

        public ArrayList<String> parseArrayList(String arrayListString) {
            arrayListString = arrayListString.replaceAll("\\[", "");
            arrayListString = arrayListString.replaceAll("\\]", "");
            ArrayList<String> arrayList = new ArrayList<>();
            String[] arrayListValues = arrayListString.split(",");
            for (int i = 0; i < arrayListValues.length; i++) {
                arrayList.add(arrayListValues[i].trim());
            }
            return arrayList;
        }

        public void reconsolidateStateObject(ArrayList<String> partitionKeys) {
            this.stateSize = 0;
            for (String partitionKey : partitionKeys) {
                String value = PathstoreClient.readState(partitionKey);
                stateSize += Long.valueOf(value.getBytes().length);
                cmap.put(partitionKey, parseArrayList(value));
            }
        }

        public void restorePartitionKeysLargeBatch(ArrayList<AtomicKey> routingKeys) {
            System.out.println("Operator has been migrated. Triggering batched restore for operator.");
            int numOfRoutingKeys = routingKeys.size();
            int batchSize = 10;
            int numOfPartitions = numOfRoutingKeys / batchSize;
            int lastOffset = numOfRoutingKeys % batchSize;
            int count = 0;
            ArrayList<String> partitionKeys = new ArrayList<>();
            for (int i = 1; i <= numOfPartitions; i++) {
                ArrayList<String> partitionKeysBatched = new ArrayList<>();
                if (i == numOfPartitions) batchSize = lastOffset;
                for (int j = 0; j < batchSize; j++) {
                    partitionKeysBatched.add(routingKeys.get(count).getValue());
                    partitionKeys.add(routingKeys.get(count).getValue());
                    count++;
                }
//                if (batchSize > 0) {
                PathstoreClient.warmupPathstore(partitionKeysBatched);
//                }
            }
            reconsolidateStateObject(partitionKeys);
        }

        public void restorePartitionKeys(ArrayList<AtomicKey> routingKeys) {
//            if (routingKeys.size() > 100) {
//                restorePartitionKeysLargeBatch(routingKeys);
//                return;
//            }
            //System.out.println("Checking / Waiting for backup to be complete for source operator");
            //PathstoreClient.pollForBackupSignal();
            //System.out.println("Backup is complete for source operator");
            //PathstoreClient.pollForBackupSignal();
            System.out.println("Operator has been migrated. Triggering restore for operator.");
            ArrayList<String> partitionKeys = new ArrayList<>();
            for (AtomicKey routingKey : routingKeys) {
                partitionKeys.add(routingKey.getValue());
            }
            long pathstoreWarmupStart = System.currentTimeMillis();
            PathstoreClient.warmupPathstore(partitionKeys);
            System.out.println("Duration for pathstore warmup:" + (System.currentTimeMillis() - pathstoreWarmupStart));
            //PathstoreClient.warmupPathstore();
            reconsolidateStateObject(partitionKeys);
        }

        public String printContents() {
            String concatenatedResult = "";
            for (String key : cmap.keySet()) {
                concatenatedResult = concatenatedResult + key + ":" + cmap.get(key).toString() + ",";
            }
            return concatenatedResult;
        }
    }

    public static class WindowStateMap {
        protected ConcurrentHashMap<String, CopyOnWriteArrayList<Integer>> cmap = new ConcurrentHashMap<>();
        private boolean writeOccurred = false;
        private CopyOnWriteArrayList<String> changedKeys = new CopyOnWriteArrayList<>();
        public String OperatorID;
        public long stateSize;
        public long changedKeysSize;

        private void updateState(ConcurrentHashMap<String, CopyOnWriteArrayList<Integer>> cmap, ArrayList<AtomicKey> routingKeys) {
            this.stateSize = 0;
            if (routingKeys != null) {
                // For on-demand backup, take backup of only keys to be routed.
                System.out.println("Performing on-demand backup of state object for " + routingKeys.size() + " keys");
                for (AtomicKey routingKey : routingKeys) {
                    String key = routingKey.getValue();
                    if (changedKeys.contains(key)) {
                        String value = cmap.get(key).toString();
                        this.stateSize += Long.valueOf(value.getBytes().length);
                        PathstoreClient.insertState(OperatorID, key, value);
                        changedKeys.remove(key);
                    }
                }
            } else {
                // For periodic backup, take backup of all keys whose values have been modified.
                ArrayList<String> changedKeysCopy = new ArrayList<>();
                changedKeysCopy.addAll(changedKeys);
                changedKeysSize = Long.valueOf(changedKeysCopy.size());
                System.out.println("Performing periodic backup of state object for " + changedKeysSize + " keys");
                changedKeysCopy.forEach((key) -> {
                    CopyOnWriteArrayList<Integer> value = cmap.get(key);
                    String valueString = value.toString();
                    this.stateSize += Long.valueOf(valueString.getBytes().length);
                    PathstoreClient.insertState(OperatorID, key, valueString);
                });
                changedKeys = new CopyOnWriteArrayList<>();
            }
        }

        public boolean checkStateChanges(ArrayList<AtomicKey> routingKeys) {
            if (writeOccurred) {
                updateState(cmap, routingKeys);
                writeOccurred = false;
                return true;
            }
            return false;
        }

        public String findIpFromAddress(String address) {
            String IPADDRESS_PATTERN =
                    "(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)";

            Pattern pattern = Pattern.compile(IPADDRESS_PATTERN);
            Matcher matcher = pattern.matcher(address);
            if (matcher.find()) {
                return matcher.group();
            }
            return "";
        }

        public WindowStateMap(String OperatorID, String node_ip_address) {
            String node_ip = findIpFromAddress(node_ip_address);
            PathstoreClient.createPropertiesFile(node_ip);
            PathstoreClient.initialiseConnection();
            this.OperatorID = OperatorID;
            EdgeStreamGetPropertyValues.setCheckpointStatus("true");

            // This is the code for periodic checkpoint.

//            new Thread(() -> {
//                do {
//                    if (EdgeStreamGetPropertyValues.getCheckpointStatus() == "true") {
//                        checkStateChanges(null);
//                    }
//                    try {
//                        Thread.sleep(500);
//                    } catch (InterruptedException e) {
//                        e.printStackTrace();
//                    }
//                } while (true);
//            }).start();

        }

        public void pausePeriodicCheckpoint() {
            new Thread(() -> {
                System.out.println("Pausing periodic checkpoint");
                EdgeStreamGetPropertyValues.setCheckpointStatus("false");
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                System.out.println("Resuming periodic checkpoint");
                EdgeStreamGetPropertyValues.setCheckpointStatus("true");
            }).start();
        }

        public ArrayList<Integer> getKey(String key) {
            return new ArrayList<>(cmap.get(key));

        }

        public void putKey(String key, ArrayList<Integer> value) {
            cmap.put(key, new CopyOnWriteArrayList<>(value));
            writeOccurred = true;
            if (!changedKeys.contains(key)) {
                changedKeys.add(key);
            }
        }

        public boolean containsKey(String key) {
            return cmap.containsKey(key);
        }

        public boolean isRestoreRequired(ArrayList<AtomicKey> routingKeys) {
            boolean restoreRequired = false;
            for (AtomicKey routingKey : routingKeys) {
                if (!routingKey.getKey().equals("any")) {
                    restoreRequired = true;
                    break;
                }
            }
            return restoreRequired;
        }

        public CopyOnWriteArrayList<Integer> parseArrayList(String arrayListString) {
            System.out.println("Output recvd from Pathstore:" + arrayListString);
            arrayListString = arrayListString.replaceAll("\\[", "");
            arrayListString = arrayListString.replaceAll("\\]", "");
            System.out.println("After substitution:" + arrayListString);
            CopyOnWriteArrayList<Integer> arrayList = new CopyOnWriteArrayList<>();
            if (arrayListString.contains(",")) {
                String[] arrayListValues = arrayListString.split(",");
                for (String arrayListValue : arrayListValues) {
                    arrayList.add(Integer.parseInt(arrayListValue.trim()));
                }
            }
            return arrayList;
        }

        public void reconsolidateStateObject(ArrayList<String> partitionKeys) {
            this.stateSize = 0;
            for (String partitionKey : partitionKeys) {
                String value = PathstoreClient.readState(partitionKey);
                cmap.put(partitionKey, parseArrayList(value));
                this.stateSize += Long.valueOf(value.getBytes().length);
            }
        }

        public void restorePartitionKeys(ArrayList<AtomicKey> routingKeys) {
            //System.out.println("Checking / Waiting for backup to be complete for source operator");
            //PathstoreClient.pollForBackupSignal();
            //System.out.println("Backup is complete for source operator");
            long startTime = System.nanoTime();
            System.out.println("Operator has been migrated. Triggering restore for operator.");
            ArrayList<String> partitionKeys = new ArrayList<>();
            for (AtomicKey routingKey : routingKeys) {
                partitionKeys.add(routingKey.getValue());
            }
            PathstoreClient.warmupPathstore(partitionKeys);
            //PathstoreClient.warmupPathstore();
            reconsolidateStateObject(partitionKeys);
            long elapsedTime = System.nanoTime() - startTime;
            System.out.println("Restore completed in: " + (double) elapsedTime / 1000000 + "ms");
        }

        public String printContents() {
            String concatenatedResult = "";
            for (String key : cmap.keySet()) {
                concatenatedResult = concatenatedResult + key + ":" + cmap.get(key).toString() + ",";
            }
            return concatenatedResult;
        }
    }

    public static class WindowStateMapWithSlide {
        protected ConcurrentHashMap<String, HashMap<String, ArrayList<Integer>>> cmap = new ConcurrentHashMap<>();
        private boolean writeOccurred = false;
        private CopyOnWriteArrayList<String> changedKeys = new CopyOnWriteArrayList<>();
        private final String OperatorID;

        private String HashMapToString(HashMap<String, ArrayList<Integer>> map) {
            String valueString = "";
            for (String key : map.keySet()) {
                valueString = valueString + key + ":" + map.get(key) + "|";
            }
            return valueString;
        }

        private void updateState(ConcurrentHashMap<String, HashMap<String, ArrayList<Integer>>> cmap, ArrayList<AtomicKey> routingKeys) {
            long startTime = System.nanoTime();
            if (routingKeys != null) {
                // For on-demand backup, take backup of only keys to be routed.
                System.out.println("Performing on-demand backup of state object for " + routingKeys.size() + " keys");
                for (AtomicKey routingKey : routingKeys) {
                    String key = routingKey.getValue();
                    if (changedKeys.contains(key)) {
                        HashMap<String, ArrayList<Integer>> value = cmap.get(key);
                        PathstoreClient.insertState(OperatorID, key, HashMapToString(value));
                        changedKeys.remove(key);
                    }
                }
            } else {
                // For periodic backup, take backup of all keys whose values have been modified.
                System.out.println("Performing periodic backup of state object for " + changedKeys.size() + " keys");
                changedKeys.forEach((key) -> {
                    HashMap<String, ArrayList<Integer>> value = cmap.get(key);
                    PathstoreClient.insertState(OperatorID, key, HashMapToString(value));
                });
                changedKeys = new CopyOnWriteArrayList<>();
            }
            long elapsedTime = System.nanoTime() - startTime;
            System.out.println("Completed backup of state object");
            System.out.println("Execution time for Backup operation: " + (float) elapsedTime / 1000000 + "ms");
        }

        public void checkStateChanges(ArrayList<AtomicKey> routingKeys) {
            if (writeOccurred) {
                updateState(cmap, routingKeys);
            }
        }

        public String findIpFromAddress(String address) {
            String IPADDRESS_PATTERN =
                    "(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)";

            Pattern pattern = Pattern.compile(IPADDRESS_PATTERN);
            Matcher matcher = pattern.matcher(address);
            if (matcher.find()) {
                return matcher.group();
            }
            return "";
        }

        public WindowStateMapWithSlide(String OperatorID, String node_ip_address) {
            String node_ip = findIpFromAddress(node_ip_address);
            PathstoreClient.createPropertiesFile(node_ip);
            PathstoreClient.initialiseConnection();
            this.OperatorID = OperatorID;
            EdgeStreamGetPropertyValues.setCheckpointStatus("true");

            // This is the code for periodic checkpoint.

            new Thread(() -> {
                do {
                    if (EdgeStreamGetPropertyValues.getCheckpointStatus() == "true") {
                        checkStateChanges(null);
                    }
                    try {
                        Thread.sleep(500);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                } while (true);
            }).start();

        }

        public void pausePeriodicCheckpoint() {
            new Thread(() -> {
                System.out.println("Pausing periodic checkpoint");
                EdgeStreamGetPropertyValues.setCheckpointStatus("false");
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                System.out.println("Resuming periodic checkpoint");
                EdgeStreamGetPropertyValues.setCheckpointStatus("true");
            }).start();
        }

        public ArrayList<Integer> getKey(String extKey, String intKey) {
            HashMap<String, ArrayList<Integer>> value = cmap.get(extKey);
            return value.get(intKey);
        }

        public void putKey(String extKey, String intKey, ArrayList<Integer> value) {
            HashMap<String, ArrayList<Integer>> valueMap = cmap.get(extKey);
            valueMap.put(intKey, value);
            cmap.put(extKey, valueMap);
            writeOccurred = true;
            changedKeys.add(extKey);
        }

        public boolean containsKey(String key) {
            return cmap.containsKey(key);
        }

        public boolean isRestoreRequired(ArrayList<AtomicKey> routingKeys) {
            boolean restoreRequired = false;
            for (AtomicKey routingKey : routingKeys) {
                if (!routingKey.getKey().equals("any")) {
                    restoreRequired = true;
                    break;
                }
            }
            return restoreRequired;
        }

        public HashMap<String, ArrayList<Integer>> parseHashMap(String hashMapString) {
            HashMap<String, ArrayList<Integer>> valueMap = new HashMap<>();
            hashMapString = hashMapString.replaceAll("\\[", "");
            hashMapString = hashMapString.replaceAll("\\]", "");
            String[] hashMapKeyValues = hashMapString.split("|");
            for (int i = 0; i < hashMapKeyValues.length - 1; i++) {
                String key = hashMapKeyValues[i].split(":")[0];
                ArrayList<Integer> value = parseArrayList(hashMapKeyValues[i].split(":")[1]);
                valueMap.put(key, value);
            }
            return valueMap;
        }

        public ArrayList<Integer> parseArrayList(String arrayListString) {
            ArrayList<Integer> arrayList = new ArrayList<>();
            String[] arrayListValues = arrayListString.split(",");
            for (int i = 0; i < arrayListValues.length; i++) {
                arrayList.add(Integer.parseInt(arrayListValues[i].trim()));
            }
            return arrayList;
        }

        public void reconsolidateStateObject(ArrayList<String> partitionKeys) {
            for (String partitionKey : partitionKeys) {
                String value = PathstoreClient.readState(partitionKey);
                cmap.put(partitionKey, parseHashMap(value));
            }
        }

        public void restorePartitionKeys(ArrayList<AtomicKey> routingKeys) {
            //System.out.println("Checking / Waiting for backup to be complete for source operator");
            //PathstoreClient.pollForBackupSignal();
            //System.out.println("Backup is complete for source operator");
            long startTime = System.nanoTime();
            System.out.println("Operator has been migrated. Triggering restore for operator.");
            ArrayList<String> partitionKeys = new ArrayList<>();
            for (AtomicKey routingKey : routingKeys) {
                partitionKeys.add(routingKey.getValue());
            }
            PathstoreClient.warmupPathstore(partitionKeys);
            //PathstoreClient.warmupPathstore();
            reconsolidateStateObject(partitionKeys);
            long elapsedTime = System.nanoTime() - startTime;
            System.out.println("Restore completed in: " + (double) elapsedTime / 1000000 + "ms");
        }

        public String printContents() {
            String concatenatedResult = "";
            for (String key : cmap.keySet()) {
                concatenatedResult = concatenatedResult + key + ":" + HashMapToString(cmap.get(key)) + ",";
            }
            return concatenatedResult;
        }
    }

    public static class WindowStateMap2 {
        protected ConcurrentHashMap<String, CopyOnWriteArrayList<String>> cmap = new ConcurrentHashMap<>();
        private boolean writeOccurred = false;
        private CopyOnWriteArrayList<String> changedKeys = new CopyOnWriteArrayList<>();
        public String OperatorID;
        public long stateSize;
        public long changedKeysSize;

        private void updateState(ConcurrentHashMap<String, CopyOnWriteArrayList<String>> cmap, ArrayList<AtomicKey> routingKeys) {
            this.stateSize = 0;
            if (routingKeys != null) {
                // For on-demand backup, take backup of only keys to be routed.
                System.out.println("Performing on-demand backup of state object for " + routingKeys.size() + " keys");
                for (AtomicKey routingKey : routingKeys) {
                    String key = routingKey.getValue();
                    if (changedKeys.contains(key)) {
                        String value = cmap.get(key).toString();
                        this.stateSize += Long.valueOf(value.getBytes().length);
                        PathstoreClient.insertState(OperatorID, key, value);
                        changedKeys.remove(key);
                    }
                }
            } else {
                // For periodic backup, take backup of all keys whose values have been modified.
                ArrayList<String> changedKeysCopy = new ArrayList<>();
                changedKeysCopy.addAll(changedKeys);
                changedKeysSize = Long.valueOf(changedKeysCopy.size());
                System.out.println("Performing periodic backup of state object for " + changedKeysSize + " keys");
                changedKeysCopy.forEach((key) -> {
                    CopyOnWriteArrayList<String> value = cmap.get(key);
                    String valueString = value.toString();
                    this.stateSize += Long.valueOf(valueString.getBytes().length);
                    PathstoreClient.insertState(OperatorID, key, valueString);
                });
                changedKeys = new CopyOnWriteArrayList<>();
            }
        }

        public boolean checkStateChanges(ArrayList<AtomicKey> routingKeys) {
            if (writeOccurred) {
                updateState(cmap, routingKeys);
                writeOccurred = false;
                return true;
            }
            return false;
        }

        public String findIpFromAddress(String address) {
            String IPADDRESS_PATTERN =
                    "(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)";

            Pattern pattern = Pattern.compile(IPADDRESS_PATTERN);
            Matcher matcher = pattern.matcher(address);
            if (matcher.find()) {
                return matcher.group();
            }
            return "";
        }

        public WindowStateMap2(String OperatorID, String node_ip_address) {
            String node_ip = findIpFromAddress(node_ip_address);
            PathstoreClient.createPropertiesFile(node_ip);
            PathstoreClient.initialiseConnection();
            this.OperatorID = OperatorID;
            EdgeStreamGetPropertyValues.setCheckpointStatus("true");

            // This is the code for periodic checkpoint.

//            new Thread(() -> {
//                do {
//                    if (EdgeStreamGetPropertyValues.getCheckpointStatus() == "true") {
//                        checkStateChanges(null);
//                    }
//                    try {
//                        Thread.sleep(500);
//                    } catch (InterruptedException e) {
//                        e.printStackTrace();
//                    }
//                } while (true);
//            }).start();

        }

        public void pausePeriodicCheckpoint() {
            new Thread(() -> {
                System.out.println("Pausing periodic checkpoint");
                EdgeStreamGetPropertyValues.setCheckpointStatus("false");
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                System.out.println("Resuming periodic checkpoint");
                EdgeStreamGetPropertyValues.setCheckpointStatus("true");
            }).start();
        }

        public ArrayList<String> getKey(String key) {
            return new ArrayList<>(cmap.get(key));

        }

        public void putKey(String key, ArrayList<String> value) {
            cmap.put(key, new CopyOnWriteArrayList<>(value));
            writeOccurred = true;
            if (!changedKeys.contains(key)) {
                changedKeys.add(key);
            }
        }

        public boolean containsKey(String key) {
            return cmap.containsKey(key);
        }

        public boolean isRestoreRequired(ArrayList<AtomicKey> routingKeys) {
            boolean restoreRequired = false;
            for (AtomicKey routingKey : routingKeys) {
                if (!routingKey.getKey().equals("any")) {
                    restoreRequired = true;
                    break;
                }
            }
            return restoreRequired;
        }

        public CopyOnWriteArrayList<String> parseArrayList(String arrayListString) {
            System.out.println("Output recvd from Pathstore:" + arrayListString);
            arrayListString = arrayListString.replaceAll("\\[", "");
            arrayListString = arrayListString.replaceAll("\\]", "");
            System.out.println("After substitution:" + arrayListString);
            CopyOnWriteArrayList<String> arrayList = new CopyOnWriteArrayList<>();
            if (arrayListString.contains(",")) {
                String[] arrayListValues = arrayListString.split(",");
                for (String arrayListValue : arrayListValues) {
                    arrayList.add(arrayListValue.trim());
                }
            }
            return arrayList;
        }

        public void reconsolidateStateObject(ArrayList<String> partitionKeys) {
            this.stateSize = 0;
            for (String partitionKey : partitionKeys) {
                String value = PathstoreClient.readState(partitionKey);
                cmap.put(partitionKey, parseArrayList(value));
                this.stateSize += Long.valueOf(value.getBytes().length);
            }
        }

        public void restorePartitionKeys(ArrayList<AtomicKey> routingKeys) {
            //System.out.println("Checking / Waiting for backup to be complete for source operator");
            //PathstoreClient.pollForBackupSignal();
            //System.out.println("Backup is complete for source operator");
            long startTime = System.nanoTime();
            System.out.println("Operator has been migrated. Triggering restore for operator.");
            ArrayList<String> partitionKeys = new ArrayList<>();
            for (AtomicKey routingKey : routingKeys) {
                partitionKeys.add(routingKey.getValue());
            }
            PathstoreClient.warmupPathstore(partitionKeys);
            //PathstoreClient.warmupPathstore();
            reconsolidateStateObject(partitionKeys);
            long elapsedTime = System.nanoTime() - startTime;
            System.out.println("Restore completed in: " + (double) elapsedTime / 1000000 + "ms");
        }

        public String printContents() {
            String concatenatedResult = "";
            for (String key : cmap.keySet()) {
                concatenatedResult = concatenatedResult + key + ":" + cmap.get(key).toString() + ",";
            }
            return concatenatedResult;
        }
    }
}
