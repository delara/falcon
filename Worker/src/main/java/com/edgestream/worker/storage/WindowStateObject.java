package com.edgestream.worker.storage;

import com.edgestream.worker.runtime.reconfiguration.state.AtomicKey;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

public class WindowStateObject {

    public static class TumblingWindowStateObject {
        int windowSize;
        // Struct: Key, List of window elements
        //HashMap<String, List<Integer>> windowState;
        StateObject.WindowStateMap windowState;
        public boolean isOperatorMigrating = false;
        public long stateSize;
        public long changedKeySize;

        public TumblingWindowStateObject(int windowSize, String OperatorID, String node_ip_address, ArrayList<AtomicKey> routingKeys) {
            this.windowSize = windowSize;
            this.windowState = new StateObject.WindowStateMap(OperatorID + "Window", node_ip_address);
            // This restores partition keys only if required. (i.e. if operator is in migrating mode.)
            isOperatorMigrating = windowState.isRestoreRequired(routingKeys);
        }

        public void restoreWindowObject(ArrayList<AtomicKey> routingKeys) {
            windowState.restorePartitionKeys(routingKeys);
            stateSize = windowState.stateSize;
            System.out.println("State after restore:" + windowState.printContents());
        }

        private void addElementToWindow(String key, Integer windowElement) {
            ArrayList<Integer> windowElements;
            if (windowState.containsKey(key)) {
                windowElements = windowState.getKey(key);
                windowElements.add(windowElement);
                windowState.putKey(key, windowElements);
            } else {
                windowElements = new ArrayList<>();
                windowElements.add(windowElement);
                windowState.putKey(key, windowElements);
            }
        }

        public void emptyWindow(String key) {
            ArrayList<Integer> windowElements = new ArrayList<>();
            windowState.putKey(key, windowElements);
        }

        private boolean checkIfWindowIsFull(String key) {
            ArrayList<Integer> windowElements = windowState.getKey(key);
            return windowElements.size() == windowSize;
        }

        public ArrayList<Integer> updateWindowState(String key, Integer windowElement) {
            addElementToWindow(key, windowElement);
            if (checkIfWindowIsFull(key)) {
                ArrayList<Integer> window = windowState.getKey(key);
                emptyWindow(key);
                return window;
            }
            return null;
        }

        public void updateWindowTimestamp(String key, String windowStartTimestamp) {
            PathstoreClient.insertState(windowState.OperatorID, key, windowStartTimestamp);
        }

        public String restoreWindowTimestamp(String key) {
            String value = PathstoreClient.readState(key);
            return value;
        }

        public void onDemandBackup(ArrayList<AtomicKey> atomicKeys, String OperatorID) {
            System.out.println("State during on-demand backup:" + windowState.printContents());
            windowState.checkStateChanges(atomicKeys);
            windowState.pausePeriodicCheckpoint();
            stateSize = windowState.stateSize;
            PathstoreClient.dumpSessionFile();
            PathstoreClient.signalBackupComplete(OperatorID + "Window");
        }

        public boolean performCheckpoint() {
            // return value checks if checkpoint actually did any data transfer
            boolean dataTransferred = windowState.checkStateChanges(null);
            stateSize = windowState.stateSize;
            changedKeySize = windowState.changedKeysSize;
            return dataTransferred;
        }

        public String printContents() {
            return windowState.printContents();
        }
    }


    public static class TumblingWindowStateObject2 {
        // This window object stores windows for composite keys.
        // Composite key means we receive a key as input key, but tuples can have additional key information
        // e.g. For Nexmark Q6, key = producer_id + bidder_id
        // We received producer_id as inputKey, we extract bidder_id from tuple
        // Now we maintain windows for (producer_id + bidder_id)
        // After reconfig, we get the routing keys which indicates the producer_ids
        // How do we know about which bidder_ids to extract for? We store this as state
        // For the window map object, we will use a special key - "registeredKeys" and the value is a list of all bidder_ids

        int windowSize;
        // Struct: Key, List of window elements
        //HashMap<String, List<Integer>> windowState;
        StateObject.WindowStateMap2 windowState;
        public boolean isOperatorMigrating = false;
        public long stateSize;
        public long changedKeySize;
        public String registeredKeysCollectionKeyName = "registeredKeys";
        ArrayList<String> registeredKeys;

        public TumblingWindowStateObject2(int windowSize, String OperatorID, String node_ip_address, ArrayList<AtomicKey> routingKeys) {
            this.windowSize = windowSize;
            this.windowState = new StateObject.WindowStateMap2(OperatorID + "Window", node_ip_address);
            registeredKeys = new ArrayList<>();
            windowState.putKey(registeredKeysCollectionKeyName, registeredKeys);
            // This restores partition keys only if required. (i.e. if operator is in migrating mode.)
            isOperatorMigrating = windowState.isRestoreRequired(routingKeys);
        }

        public ArrayList<String> parseArrayList(String arrayListString) {
            System.out.println("Output recvd from Pathstore:" + arrayListString);
            arrayListString = arrayListString.replaceAll("\\[", "");
            arrayListString = arrayListString.replaceAll("\\]", "");
            System.out.println("After substitution:" + arrayListString);
            ArrayList<String> arrayList = new ArrayList<>();
            if (arrayListString.contains(",")) {
                String[] arrayListValues = arrayListString.split(",");
                for (String arrayListValue : arrayListValues) {
                    arrayList.add(arrayListValue.trim());
                }
            }
            return arrayList;
        }

        public void reconstructRegisteredKeys() {
            String registeredKeysString = PathstoreClient.readState(registeredKeysCollectionKeyName);
            registeredKeys = parseArrayList(registeredKeysString);
            windowState.putKey(registeredKeysCollectionKeyName, registeredKeys);
//            registeredKeys = windowState.getKey(registeredKeysCollectionKeyName);
        }

        public void restorePartitionKeys(ArrayList<AtomicKey> routingKeys) {
            long startTime = System.nanoTime();
            System.out.println("Operator has been migrated. Triggering restore for operator.");
            for (AtomicKey routingKey : routingKeys) {
                String partitionKey = routingKey.getValue();
                String value = PathstoreClient.readState(partitionKey);
                if (value != null) {
                    windowState.putKey(partitionKey, parseArrayList(value));
                } else {
                    System.out.println("Received null value for key:" + partitionKey);
                }
            }
            long elapsedTime = System.nanoTime() - startTime;
            System.out.println("Restore completed in: " + (double) elapsedTime / 1000000 + "ms");
        }

        public void restoreWindowObject() {
            reconstructRegisteredKeys();
            ArrayList<AtomicKey> partitionKeys = new ArrayList<>();
            for (String key: registeredKeys) {
                partitionKeys.add(new AtomicKey("", key));
            }
            restorePartitionKeys(partitionKeys);
            stateSize = windowState.stateSize;
            System.out.println("State after restore:" + windowState.printContents());
        }

        private void addElementToWindow(String key, String windowElement) {
            ArrayList<String> windowElements;
            if (windowState.containsKey(key)) {
                windowElements = windowState.getKey(key);
                windowElements.add(windowElement);
                windowState.putKey(key, windowElements);
            } else {
                windowElements = new ArrayList<>();
                windowElements.add(windowElement);
                windowState.putKey(key, windowElements);
            }
        }

        public void emptyWindow(String key) {
            ArrayList<String> windowElements = new ArrayList<>();
            windowState.putKey(key, windowElements);
        }

        private boolean checkIfWindowIsFull(String key) {
            ArrayList<String> windowElements = windowState.getKey(key);
            return windowElements.size() == windowSize;
        }

        private void addKeyToRegisteredKeys(String key) {
            if (!registeredKeys.contains(key)) {
                registeredKeys.add(key);
            }
            windowState.putKey(registeredKeysCollectionKeyName, registeredKeys);
        }

        public ArrayList<String> updateWindowState(String key, String windowElement) {
            addElementToWindow(key, windowElement);
            addKeyToRegisteredKeys(key);
            if (checkIfWindowIsFull(key)) {
                ArrayList<String> window = windowState.getKey(key);
                emptyWindow(key);
                return window;
            }
            return null;
        }

        public void onDemandBackup(ArrayList<AtomicKey> atomicKeys, String OperatorID) {
            System.out.println("State during on-demand backup:" + windowState.printContents());
            windowState.checkStateChanges(atomicKeys);
            windowState.pausePeriodicCheckpoint();
            stateSize = windowState.stateSize;
            PathstoreClient.dumpSessionFile();
            PathstoreClient.signalBackupComplete(OperatorID + "Window");
        }

        public boolean performCheckpoint() {
            // return value checks if checkpoint actually did any data transfer
            boolean dataTransferred = windowState.checkStateChanges(null);
            stateSize = windowState.stateSize;
            changedKeySize = windowState.changedKeysSize;
            return dataTransferred;
        }

        public String printContents() {
            return windowState.printContents();
        }
    }

    public static class SlidingWindowStateObject {
        int windowSize;
        int slideSize;
        String windowKey = "window";
        String slideKey = "slide";
        // Struct: Key, List of window elements
        //HashMap<String, List<Integer>> windowState;
        StateObject.WindowStateMapWithSlide windowAndSlideState;
        public boolean isOperatorMigrating = false;

        public SlidingWindowStateObject(int windowSize, int slideSize, String OperatorID, String node_ip_address, ArrayList<AtomicKey> routingKeys) {
            this.windowSize = windowSize;
            this.slideSize = slideSize;
            this.windowAndSlideState = new StateObject.WindowStateMapWithSlide(OperatorID + "Window", node_ip_address);
            // This restores partition keys only if required. (i.e. if operator is in migrating mode.)
            isOperatorMigrating = windowAndSlideState.isRestoreRequired(routingKeys);
            if (isOperatorMigrating) {
                windowAndSlideState.restorePartitionKeys(routingKeys);
                System.out.println("State after restore:" + windowAndSlideState.printContents());
            }
        }

        private void addElementToWindowOrSlide(String key, String windowOrSlideKey, Integer windowElement) {
            ArrayList<Integer> windowElements;
            if (windowAndSlideState.containsKey(key)) {
                windowElements = windowAndSlideState.getKey(key, windowOrSlideKey);
                windowElements.add(windowElement);
                windowAndSlideState.putKey(key, windowOrSlideKey, windowElements);
            } else {
                emptyWindowOrSlide(key, windowKey);
                emptyWindowOrSlide(key, slideKey);
                windowElements = new ArrayList<>();
                windowElements.add(windowElement);
                windowAndSlideState.putKey(key, windowOrSlideKey, windowElements);
            }
        }

        public void emptyWindowOrSlide(String key, String windowOrSlideKey) {
            ArrayList<Integer> windowElements = new ArrayList<>();
            windowAndSlideState.putKey(key, windowOrSlideKey, windowElements);
        }

        private boolean checkIfWindowOrSlideIsFull(String key, String windowOrSlideKey) {
            ArrayList<Integer> elements = windowAndSlideState.getKey(key, windowOrSlideKey);
            if (windowOrSlideKey == windowKey) {
                return elements.size() == windowSize;
            } else {
                return elements.size() == slideSize;
            }
        }

//        private void insertSlideIntoWindow(String key) {
//            int currentWindowSize = windowAndSlideState.getKey(key, windowKey).size();
//            if (currentWindowSize + slideSize < windowSize) {
//                ArrayList<Integer> slideElements = windowAndSlideState.getKey(key, slideKey);
//                for (Integer slideElement)
//            }
//        }

//        public List<Integer> updateWindowState(String key, Integer element) {
//            addElementToWindowOrSlide(key, slideKey, element);
//            if (checkIfWindowOrSlideIsFull(key, slideKey)) {
//                insertSlideIntoWindow(key);
//            }
//            addElementToWindow(key, windowElement);
//            if (checkIfWindowIsFull(key)) {
//                ArrayList<Integer> window = windowAndSlideState.getKey(key);
//                emptyWindow(key);
//                return window;
//            }
//            return null;
//        }

        public void onDemandBackup(ArrayList<AtomicKey> atomicKeys, String OperatorID) {
            System.out.println("State during on-demand backup:" + windowAndSlideState.printContents());
            windowAndSlideState.checkStateChanges(atomicKeys);
            windowAndSlideState.pausePeriodicCheckpoint();
            PathstoreClient.dumpSessionFile();
            PathstoreClient.signalBackupComplete(OperatorID + "Window");
        }

        public String printContents() {
            return windowAndSlideState.printContents();
        }
    }
}
