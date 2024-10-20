package com.edgestream.worker.storage;

import com.edgestream.worker.common.Tuple;
import com.edgestream.worker.runtime.reconfiguration.state.AtomicKey;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Array;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class EventTimeWindowManager {
    long windowFreqInMillis;
    HashMap<String, EventTimeWindowStateObject> windowStateObjForAllKeys = new HashMap<>();
    EventTimeWindowStateObject windowStateObject;
    boolean isOperatorMigrating;
    String OperatorID;
    // Apart from the window state; the UDF itself could also store state per key
    public HashMap<String, String> operatorState = new HashMap<>();

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

//    public void updateOperatorState(String key, String value) {
//        operatorState.put(key, value);
//    }

    public long calculateWindowStartTime(long timeStampInMillis) {
        long offset = timeStampInMillis % windowFreqInMillis;
        long windowStartTime = timeStampInMillis - offset;
        return windowStartTime;
    }

    public void createNewWindow(long timeStampInMillis) {
        long windowStartTime = calculateWindowStartTime(timeStampInMillis);
        System.out.println("Window start time:" + windowStartTime);
        windowStateObject.windowStartTimes.add(windowStartTime);
    }

    public EventTimeWindowManager(long windowFreqInSec, ArrayList<AtomicKey> routingKeys, String OperatorID, String node_ip_address) {
        this.windowFreqInMillis = windowFreqInSec * 1000;
        isOperatorMigrating = isRestoreRequired(routingKeys);
        String node_ip = findIpFromAddress(node_ip_address);
        PathstoreClient.createPropertiesFile(node_ip);
        PathstoreClient.initialiseConnection();
        this.OperatorID = OperatorID;
    }

    public boolean isRestoreRequired(ArrayList<AtomicKey> routingKeys) {
        boolean restoreRequired = false;
        for (AtomicKey routingKey: routingKeys) {
            if (!routingKey.getKey().equals("any")) {
                restoreRequired = true;
                break;
            }
        }
        return restoreRequired;
    }

    private boolean isFirstTuple(String key) {
        return !windowStateObjForAllKeys.containsKey(key);
    }

    private EventTimeWindowStateObject initialiseWindowStateObjForKey(String key) {
        EventTimeWindowStateObject windowStateObject = new EventTimeWindowStateObject(key);
        return windowStateObject;
    }

    private boolean isSourceAlreadyRegistered(String sourceId) {
        return windowStateObject.registeredSources.contains(sourceId);
    }

    private void registerSource(String sourceId) {
        windowStateObject.registeredSources.add(sourceId);
        windowStateObject.sourceWatermarks.put(sourceId, new ArrayList<>());
        windowStateObject.sourceWindows.put(sourceId, new ArrayList<>());
    }

    private void updateWindowStateObjectForKey(String key, EventTimeWindowStateObject windowStateObject) {
        windowStateObjForAllKeys.put(key, windowStateObject);
    }

    private boolean checkIfSourceWatermarkExists(String sourceId, int i) {
        if (windowStateObject.sourceWatermarks.get(sourceId).size() > i) {
            return windowStateObject.sourceWatermarks.get(sourceId).get(i) == "W";
        }
        return false;
    }

    private void addTupleToSourceWindow(String sourceId, String tupleData) {
        ArrayList<String> sourceWindow = windowStateObject.sourceWindows.get(sourceId);
        sourceWindow.add(tupleData);
        windowStateObject.sourceWindows.put(sourceId, sourceWindow);
    }

    private void addWatermarkToSourceWatermarks(String sourceId) {
        windowStateObject.sourceWatermarks.get(sourceId).add("W");
    }

    private void addWatermarkToSourceWindow(String sourceId) {
        windowStateObject.sourceWindows.get(sourceId).add("W");
    }

    private boolean checkIfAllWindowsClosed(int i) {
        for (String sourceId: windowStateObject.sourceWatermarks.keySet()) {
            if (windowStateObject.sourceWatermarks.get(sourceId).size() <= i) {
                return false;
            } else if (windowStateObject.sourceWatermarks.get(sourceId).get(i) != "W") {
                return false;
            }
        }
        return true;
    }

    private ArrayList<String> collectAllWindows() {
        ArrayList<String> allWindowElements = new ArrayList<>();
        for (String sourceId: windowStateObject.sourceWindows.keySet()) {
            // Read and extract the first element in the queue, until you reach W (incl. W)
            String windowElement = windowStateObject.sourceWindows.get(sourceId).get(0);
            windowStateObject.sourceWindows.get(sourceId).remove(0);
            while(windowElement!="W") {
                allWindowElements.add(windowElement);
                windowElement = windowStateObject.sourceWindows.get(sourceId).get(0);
                windowStateObject.sourceWindows.get(sourceId).remove(0);
            }
        }
        return allWindowElements;
    }

    public String getTupleData(Tuple tuple) {
        String tuplePayload = new String(tuple.getPayloadAsByteArray());
        return tuplePayload;
    }

    public void checkAndWriteOperatorState() {
        for (String key: operatorState.keySet()) {
            PathstoreClient.insertState(OperatorID, key, operatorState.get(key));
        }
    }

    public void checkAndWriteSources(String key, EventTimeWindowStateObject windowStateObject) {
        String value = windowStateObject.registeredSources.toString();
//        String tableName = "sourcestable";
//        PathstoreClient.insertStateWithTableName(OperatorID, key, value, tableName);
        key += "_S";
        PathstoreClient.insertState(OperatorID, key, value);
    }

    public void checkAndWriteWindowStartTimes(String key, EventTimeWindowStateObject windowStateObject) {
        String value = windowStateObject.windowStartTimes.toString();
//        String tableName = "windowstarttimestatetable";
//        PathstoreClient.insertStateWithTableName(OperatorID, key, value, tableName);
        key += "_WS";
        PathstoreClient.insertState(OperatorID, key, value);
    }

    public void checkAndWriteSourceWatermarks(String key, EventTimeWindowStateObject windowStateObject) {
        for (String source: windowStateObject.registeredSources) {
            String value = windowStateObject.sourceWatermarks.get(source).toString();
//            String tableName = "sourcewatermarkstable";
//            PathstoreClient.insertStateWithTableName(OperatorID, key+source, value, tableName);
            key = key + source + "_SWat";
            PathstoreClient.insertState(OperatorID, key, value);
        }
    }

    public void checkAndWriteSourceWindows(String key, EventTimeWindowStateObject windowStateObject) {
        for (String source: windowStateObject.registeredSources) {
            String value = windowStateObject.sourceWindows.get(source).toString();
//            String tableName = "sourcewindowstable";
//            PathstoreClient.insertStateWithTableName(OperatorID, key+source, value, tableName);
            key = key + source + "_SWin";
            PathstoreClient.insertState(OperatorID, key, value);
        }
    }

    public ArrayList<String> parseArrayListString(String arrayListString) {
        arrayListString = arrayListString.replaceAll("\\[", "");
        arrayListString = arrayListString.replaceAll("\\]", "");
        ArrayList<String> arrayList = new ArrayList<>();
        String[] arrayListValues = arrayListString.split(",");
        for (int i = 0; i < arrayListValues.length; i++) {
            arrayList.add(arrayListValues[i].trim());
        }
        return arrayList;
    }

    public ArrayList<Long> parseArrayListLong(String arrayListString) {
        arrayListString = arrayListString.replaceAll("\\[", "");
        arrayListString = arrayListString.replaceAll("\\]", "");
        ArrayList<Long> arrayList = new ArrayList<>();
        String[] arrayListValues = arrayListString.split(",");
        for (int i = 0; i < arrayListValues.length; i++) {
            arrayList.add(Long.parseLong(arrayListValues[i].trim()));
        }
        return arrayList;
    }

    // This will retrieve the string. If UDF needs parsing; we have to change this logic.
    public void getOperatorState(String key) {
        String value = PathstoreClient.readState(key);
        System.out.println("Operator state restored from Pathstore for key: " + key + " is " + value);
        operatorState.put(key, value);
    }

    public ArrayList<String> getSources(String key) {
        key += "_S";
        String stateVal = PathstoreClient.readState(key);
        System.out.println("Sources received from Pathstore:" + stateVal);
        return parseArrayListString(stateVal);
    }

    public ArrayList<Long> getWindowStartTimes(String key) {
        key += "_WS";
        String stateVal = PathstoreClient.readState(key);
        return parseArrayListLong(stateVal);
    }

    public HashMap<String, ArrayList<String>> getSourceWatermarks(String key, ArrayList<String> sources) {
        HashMap<String, ArrayList<String>> sourceWatermarks = new HashMap<>();
        for (String source: sources) {
            key = key + source + "_SWat";
            String stateVal = PathstoreClient.readState(key);
            sourceWatermarks.put(source, parseArrayListString(stateVal));
        }
        return sourceWatermarks;
    }

    public HashMap<String, ArrayList<String>> getSourceWindows(String key, ArrayList<String> sources) {
        HashMap<String, ArrayList<String>> sourceWindows = new HashMap<>();
        for (String source: sources) {
            key = key + source + "_SWin";
            String stateVal = PathstoreClient.readState(key);
            sourceWindows.put(source, parseArrayListString(stateVal));
        }
        return sourceWindows;
    }

    public void performOnDemandBackup(ArrayList<AtomicKey> routingKeys) {
        for(AtomicKey routingKey: routingKeys) {
            String key = routingKey.getValue();
            EventTimeWindowStateObject windowStateObject = windowStateObjForAllKeys.get(key);
            //Need to check if the values actually changed for the key
            checkAndWriteOperatorState();
            checkAndWriteSources(key, windowStateObject);
            checkAndWriteWindowStartTimes(key, windowStateObject);
            checkAndWriteSourceWatermarks(key, windowStateObject);
            checkAndWriteSourceWindows(key, windowStateObject);
        }
        PathstoreClient.dumpSessionFile();
    }

    public void performRestore(ArrayList<AtomicKey> routingKeys) {
        ArrayList<String> partitionKeys = new ArrayList<>();
        ArrayList<String> keysWithSources = new ArrayList<>();
        for (AtomicKey routingKey: routingKeys) {
            String key = routingKey.getValue();
            partitionKeys.add(key);
            partitionKeys.add(key+"_S");
            partitionKeys.add(key+"_WS");
        }
//        ArrayList<String> tableNames = new ArrayList<>();
//        tableNames.add("timestatetable");
//        tableNames.add("sourcestable");
//        tableNames.add("windowstarttimestatetable");
//        PathstoreClient.warmupPathstoreForMultipleTables(partitionKeys, tableNames);
        PathstoreClient.warmupPathstore(partitionKeys);
        for (AtomicKey routingKey: routingKeys) {
            String key = routingKey.getValue();
            ArrayList<String> sourcesForKey = getSources(key);
            for (String source: sourcesForKey) {
                keysWithSources.add(key+source+"_SWat");
                keysWithSources.add(key+source+"_SWin");
            }
        }
//        tableNames = new ArrayList<>();
//        tableNames.add("sourcewatermarkstable");
//        tableNames.add("sourcewindowstable");
//        PathstoreClient.warmupPathstoreForMultipleTables(keysWithSources, tableNames);
        PathstoreClient.warmupPathstore(keysWithSources);
        for(AtomicKey routingKey: routingKeys) {
            String key = routingKey.getValue();
            EventTimeWindowStateObject windowStateObject = new EventTimeWindowStateObject(key);
            windowStateObject.registeredSources = getSources(key);
            windowStateObject.windowStartTimes = getWindowStartTimes(key);
            windowStateObject.sourceWatermarks = getSourceWatermarks(key, windowStateObject.registeredSources);
            windowStateObject.sourceWindows = getSourceWindows(key, windowStateObject.registeredSources);
            windowStateObjForAllKeys.put(key, windowStateObject);
        }
    }

    public ArrayList<String> processTuple(Tuple tuple, String producerId, String timeStamp, String inputKey) {
        if (tuple.isReconfigMarker()) {
            System.out.println("Received reconfig marker");
            if (isOperatorMigrating) {
                System.out.println("Reconfiguration request received. Initiating restore.");
                //System.out.println("Tuple routing keys: " + ReflectionToStringBuilder.toString(tuple.getTupleHeader().getRoutingKeys()));
                //Perform backup operation for only keys to be routed.
//                try {
//                    Thread.sleep(1000);
//                } catch (InterruptedException e) {
//                    e.printStackTrace();
//                }
                performRestore(tuple.getTupleHeader().getAtomicKeys());
                System.out.println("Restore complete");
                System.out.println("State during restore:" + printAllContent(tuple.getTupleHeader().getAtomicKeys()));
                return null;
            } else {
                System.out.println("Reconfiguration request received. Initiating on-demand backup.");
                //System.out.println("Tuple routing keys: " + ReflectionToStringBuilder.toString(tuple.getTupleHeader().getRoutingKeys()));
                //Perform backup operation for only keys to be routed.
                performOnDemandBackup(tuple.getTupleHeader().getAtomicKeys());
                System.out.println("On-demand backup complete");
                System.out.println("State during on-demand backup:" + printAllContent(tuple.getTupleHeader().getAtomicKeys()));
                return null;
            }
        }
        String tupleData = getTupleData(tuple);
        ArrayList <String> windowElements = new ArrayList<>();
        long timeStampInMillis = ZonedDateTime.parse(timeStamp).toInstant().toEpochMilli();
        System.out.println("Processing tuple having timestamp:" + timeStampInMillis);
//        long timeStampInMillis = Long.parseLong(timeStamp);

        if (isFirstTuple(inputKey)) {
            System.out.println("Processing first tuple for key: " + inputKey);
            windowStateObject = initialiseWindowStateObjForKey(inputKey);
            createNewWindow(timeStampInMillis);
        } else {
            windowStateObject = windowStateObjForAllKeys.get(inputKey);
        }
        String sourceId = producerId;
        if (!isSourceAlreadyRegistered(sourceId)) {
            System.out.println("Registering source " + sourceId + " for key " + inputKey);
            registerSource(sourceId);
        }
        // Remember that windowStartTimes are created by a concurrent thread.
        // Fetching this in the beginning to avoid uncertainty in for loop
        int num_of_windows = windowStateObject.windowStartTimes.size();
        for (int i=0;i<num_of_windows; i++) {
            if (timeStampInMillis>(windowStateObject.windowStartTimes.get(i) + windowFreqInMillis)) {
                if (!checkIfSourceWatermarkExists(sourceId, i)) {
                    System.out.println("Closing window " + i + " for source " + sourceId + " for key " + inputKey);
                    addWatermarkToSourceWatermarks(sourceId);
                    addWatermarkToSourceWindow(sourceId);
                    if (i+1 == num_of_windows) {
                        // Tuple time is greater than all existing window end times
                        // Need to create a new window
                        createNewWindow(timeStampInMillis);
                    }
                    if (checkIfAllWindowsClosed(i)) {
                        System.out.println("All windows are closed for " + i + " for key " + inputKey);
                        windowElements = collectAllWindows();
                    }
                }
            } else {
                System.out.println("Adding tuple to window for source " + sourceId + " for key " + inputKey);
                addTupleToSourceWindow(sourceId, tupleData);
                break;
            }
        }
        updateWindowStateObjectForKey(inputKey, windowStateObject);
        if (windowElements.size() == 0) { return null; }
        return windowElements;
    }

    public String printAllContent(ArrayList<AtomicKey> routingKeys) {
        String value = "";
        for(AtomicKey routingKey: routingKeys) {
            String key = routingKey.getValue();
            value += ReflectionToStringBuilder.toString(windowStateObjForAllKeys.get(key));
        }
        return value;
    }

    public String printContent() {
        try {
//            ByteArrayOutputStream bos = new ByteArrayOutputStream();
//            ObjectOutputStream oos = new ObjectOutputStream(bos);
//            oos.writeObject(windowStateObject);
//            String serialised_object_string = bos.toString();
//            oos.close();
//            return serialised_object_string;
            return ReflectionToStringBuilder.toString(windowStateObject);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return "Could not convert object to string";
    }
}
