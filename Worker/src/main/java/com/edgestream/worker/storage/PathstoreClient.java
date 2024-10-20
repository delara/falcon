package com.edgestream.worker.storage;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.querybuilder.Update;
//import com.datastax.driver.core.Statement;
//import com.edgestream.worker.operator.OperatorID;
//import com.edgestream.worker.runtime.docker.DockerFile;
//import com.edgestream.worker.runtime.docker.DockerFileType;
import pathstore.client.PathStoreClientAuthenticatedCluster;
import pathstore.client.PathStoreResultSet;
import pathstore.client.PathStoreSession;
import pathstore.sessions.PathStoreSessionManager;
import pathstore.sessions.SessionToken;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.ResultSet;
import java.util.*;

public class PathstoreClient {
    static PathStoreSession globalSession;

    public static void testPathStore() throws IOException {
        PathStoreSession session = PathStoreClientAuthenticatedCluster.getInstance().pathStoreSession();
        System.out.println("Initiating testing of pathstore");

        Insert insert =
                QueryBuilder.insertInto("pathstore_demo", "users")
                        .value("name", "pritish2")
                        .value("sport", "cricket2");

        session.execute(insert, PathStoreSessionManager.getInstance().getKeyspaceToken("demo-session"));

        Select select = QueryBuilder.select().all().from("pathstore_demo", "users");
        for (Row row :
                session.execute(
                        select, PathStoreSessionManager.getInstance().getKeyspaceToken("demo-session"))) {
            System.out.println(String.format("%s %s", row.getString("name"), row.getString("sport")));
        }

        PathStoreClientAuthenticatedCluster.getInstance().close();

        PathStoreSessionManager.getInstance().close();
    }

    /* This function tests the connection to pathstore and executes a read and a write query.
    The test is meant to be run against edgestream operator table.
     */
    public static void testPathStore2() throws IOException {
        PathStoreSession session = PathStoreClientAuthenticatedCluster.getInstance().pathStoreSession();
        System.out.println("Initiating testing of pathstore for edgestream");

        Insert insert =
                QueryBuilder.insertInto("pathstore_edgestream", "opstatetable")
                        .value("op_id", "OP001")
                        .value("partition_key", "some_random_key")
                        .value("state_val", "some_random_value");

        SessionToken sessionToken = PathStoreSessionManager.getInstance().getKeyspaceToken("edge-session");

        session.execute(insert, sessionToken);

        Select select = QueryBuilder.select().all().from("pathstore_edgestream", "opstatetable");
        for (Row row :
                session.execute(
                        select, PathStoreSessionManager.getInstance().getKeyspaceToken("edge-session"))) {
            System.out.println(String.format("%s %s %s", row.getString("op_id"), row.getString("partition_key"), row.getString("state_val")));
        }

        initialiseConnection();
        System.out.println("Result of partitionKeyExists: " + partitionKeyExists("abc"));
        System.out.println("Value of some_random_key: " + readState("some_random_key"));
        updateState("some_random_key", "some_other_value");
        System.out.println("Value of some_random_key: " + readState("some_random_key"));
        insertState("OP001", "second_random_key", "some_random_value");
        System.out.println("Value of second_random_key: " + readState("second_random_key"));


        PathStoreClientAuthenticatedCluster.getInstance().close();

        PathStoreSessionManager.getInstance().close();
    }

    public static void initialiseConnection() {
        globalSession = PathStoreClientAuthenticatedCluster.getInstance().pathStoreSession();
    }

    public static void signalBackupComplete(String OperatorID) {
        System.out.println("Backup Complete. Signalling replicated operator.");
        String key = "backup_signal";
        String value = "success";
        insertState(OperatorID, key, value);
    }

    public static void eraseBackupSignal() {
        String OperatorID = getOperatorId("backup_signal");
        System.out.println("Erasing backup signal for future uses.");
        String key = "backup_signal";
        String value = "dummy";
        insertState(OperatorID, key, value);
    }

    public static boolean isBackupComplete() {
        System.out.println("Reading backup state");
        String val = readState("backup_signal");
        if (val != null && val.equals("success")) {
            System.out.println("Backup state value:" + val);
            return true;
        } else {
            return false;
        }
    }

    public static void pollForBackupSignal() {
        System.out.println("Checking if backup is complete");
        while (!isBackupComplete()) {
            try {
                System.out.println("Backup incomplete. Will check again.");
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public static void storeSessionFileContent() {
        File file = new File(System.getProperty("user.home") + "/sessionFile.txt");
        String fileContent = "";
        try {
            Scanner sc = new Scanner(file);
            while (sc.hasNextLine()) {
                String lineContent = sc.nextLine();
                fileContent += "newline"+ lineContent;
            }
        } catch (IOException e) {
            // do something
            e.printStackTrace();
        }
        String key = "session_file_value";
        String value = fileContent;
        String OperatorID = "OP001";
//        if (partitionKeyExists(key.toString())) {
//            updateState(key.toString(), value.toString());
//        } else {
            insertState(OperatorID, key, value);
        //}
//        Insert insert =
//                QueryBuilder.insertInto("pathstore_edgestream", "opstatetable")
//                        .value("op_id", "OP001")
//                        .value("partition_key", "session_file_value")
//                        .value("state_val", fileContent);
//        SessionToken sessionToken = PathStoreSessionManager.getInstance().getKeyspaceToken( "session_file_value" + "-session");
//        globalSession.execute(insert, sessionToken);
    }

    public static void retrieveSessionFileContent() {
        String fileContent = null;
        SessionToken sessionToken = PathStoreSessionManager.getInstance().getKeyspaceToken("session_file_value" + "-session");
        Select select = QueryBuilder.select()
                .all()
                .from("pathstore_edgestream", "opstatetable");
        select.where(QueryBuilder.eq("partition_key", "session_file_value"));
        for (Row row : globalSession.execute(select, sessionToken)) {
            fileContent = row.getString("state_val");
        }
        String[] lines = fileContent.split("newline");
        boolean firstLine = true;
        try {
            File newFile = new File(System.getProperty("user.home") + "/sessionFile.txt");
            FileWriter writer = new FileWriter(newFile);
            for (String line: lines) {
                if (firstLine) {firstLine= false; continue;}
                writer.write(line+"\n");
            }
            writer.close();
        } catch (IOException e) {
            // do something
            e.printStackTrace();
        }
    }

//    public static void warmupPathstore() {
//        String[] tupleTypes = {"humidity", "airquality_raw", "light", "temperature", "dust"};
//        String[] sensorIds = PartitionKeysSet.getSensorIds();
//        for (String sensorId: sensorIds) {
//            sensorId = sensorId.trim();
//            for (String tupleType: tupleTypes) {
//                String partitionKey = sensorId+tupleType;
//                String stateVal = readState(partitionKey);
//                System.out.println("Key: " + partitionKey + " Value: " + stateVal);
//            }
//        }
//    }

    public static String[] getAllPartitionKeys() {
        String[] tupleTypes = {"humidity", "airquality_raw", "light", "temperature", "dust"};
        String[] sensorIds = PartitionKeysSet.getSensorIds();
        ArrayList<String> partitionKeysList = new ArrayList<>();
        for (String sensorId : sensorIds) {
            sensorId = sensorId.trim();
            for (String tupleType : tupleTypes) {
                String partitionKey = sensorId + tupleType;
                partitionKeysList.add(partitionKey);
            }
        }
        return (String[]) partitionKeysList.toArray();
    }

    public static void warmupPathstore() {
        String[] tupleTypes = {"humidity", "airquality_raw", "light", "temperature", "dust"};
        String[] sensorIds = PartitionKeysSet.getSensorIds();
        System.out.println("Triggering restore for " + sensorIds.length * tupleTypes.length + " keys" );
        ArrayList<Select> SelectStats = new ArrayList<>();
        ArrayList<SessionToken> SessionTokens = new ArrayList<>();
        for (String sensorId: sensorIds) {
            sensorId = sensorId.trim();
            for (String tupleType: tupleTypes) {
                String partitionKey = sensorId+tupleType;
                SessionToken sessionToken = PathStoreSessionManager.getInstance().getKeyspaceToken(partitionKey + "-session");
                Select select = QueryBuilder.select()
                        .all()
                        .from("pathstore_edgestream", "opstatetable");
                select.where(QueryBuilder.eq("partition_key", partitionKey));
                SelectStats.add(select);
                SessionTokens.add(sessionToken);
            }
        }
        globalSession.reConsolidateAndReLocate(SessionTokens, SelectStats, "pathstore_edgestream", "opstatetable");
    }

    public static void warmupPathstore(ArrayList<String> partitionKeys) {
        System.out.println("Triggering restore for " + partitionKeys.size() + " keys");
        ArrayList<Select> SelectStats = new ArrayList<>();
        ArrayList<SessionToken> SessionTokens = new ArrayList<>();
        for ( String partitionKey: partitionKeys) {
            SessionToken sessionToken = PathStoreSessionManager.getInstance().getKeyspaceToken(partitionKey + "-session");
            Select select = QueryBuilder.select()
                    .all()
                    .from("pathstore_edgestream", "opstatetable");
            select.where(QueryBuilder.eq("partition_key", partitionKey));
            SelectStats.add(select);
            SessionTokens.add(sessionToken);
        }
        globalSession.reConsolidateAndReLocate(SessionTokens, SelectStats, "pathstore_edgestream", "opstatetable");
        System.out.println("Restore complete");
    }

    public static void warmupPathstoreForMultipleTables(ArrayList<String> partitionKeys, ArrayList<String> tableNames) {
        ArrayList<Select> SelectStats = new ArrayList<>();
        ArrayList<SessionToken> SessionTokens = new ArrayList<>();
        for (String tableName: tableNames) {
            System.out.println("Triggering restore for " + partitionKeys + " keys for table " + tableName);
            for ( String partitionKey: partitionKeys) {
                SessionToken sessionToken = PathStoreSessionManager.getInstance().getKeyspaceToken(partitionKey + "-session");
                Select select = QueryBuilder.select()
                        .all()
                        .from("pathstore_edgestream", tableName);
                select.where(QueryBuilder.eq("partition_key", partitionKey));
                SelectStats.add(select);
                SessionTokens.add(sessionToken);
            }
            globalSession.reConsolidateAndReLocate(SessionTokens, SelectStats, "pathstore_edgestream", tableName);
        }
    }

    public static void dumpSessionFile() {
        try {
            PathStoreSessionManager.getInstance().swap();
        } catch (IOException e) {
            e.printStackTrace();
        }
        storeSessionFileContent();
    }

    public static void createPropertiesFile(String node_ip) {
        String fileContent = "Role=CLIENT\n" +
                "CassandraPort=9052\n" +
                "RMIRegistryIP="+ node_ip +"\n" +
                "CassandraIP="+ node_ip + "\n" +
                "RMIRegistryPort=1099\n" +
                "sessionFile=" + System.getProperty("user.home") + "/sessionFile.txt\n" +
                "applicationName=pathstore_edgestream\n" +
                "applicationMasterPassword=edgestream";
        File file = new File("/etc/pathstore");
        boolean bool = file.mkdirs();
        System.out.println(bool);
        file = new File("/etc/pathstore/pathstore.properties");
        try {
            FileWriter writer = new FileWriter(file);
            writer.write(fileContent);
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static StateObject.OperatorState readOperatorState(String OperatorId) {
        SessionToken sessionToken = PathStoreSessionManager.getInstance().getKeyspaceToken("edge-session");
        Select.Where select = QueryBuilder.select()
                .all()
                .from("pathstore_edgestream", "opstatetable")
                .where(QueryBuilder.eq("op_id", OperatorId));
        StateObject.OperatorState operatorState = new StateObject.OperatorState();
        Long partitionKey;
        String stateVal;
        int i = 0;
        for (Row row : globalSession.execute(select, sessionToken)) {
            partitionKey = Long.parseLong(row.getString("partition_key"));
            stateVal = row.getString("state_val");
            operatorState.partitionKeys.add(i, partitionKey);
            operatorState.stateValues.add(i, stateVal);
            i++;
        }
        return operatorState;
    }

    public static String getOperatorId(String key) {
        String operatorId = null;
        SessionToken sessionToken = PathStoreSessionManager.getInstance().getKeyspaceToken(key + "-session");
        Select select = QueryBuilder.select()
                .all()
                .from("pathstore_edgestream", "opstatetable");
        select.where(QueryBuilder.eq("partition_key", key));
        for (Row row : globalSession.execute(select, sessionToken)) {
            operatorId = row.getString("op_id");
        }
        return operatorId;
    }

    public static String readState(String key) {
        String stateVal = null;
        SessionToken sessionToken = PathStoreSessionManager.getInstance().getKeyspaceToken(key + "-session");
        Select select = QueryBuilder.select()
                .all()
                .from("pathstore_edgestream", "opstatetable");
        select.where(QueryBuilder.eq("partition_key", key));
        for (Row row : globalSession.execute(select, sessionToken)) {
            stateVal = row.getString("state_val");
        }
        return stateVal;
    }

    public static String readStateWithTableName(String key, String tableName) {
        String stateVal = null;
        SessionToken sessionToken = PathStoreSessionManager.getInstance().getKeyspaceToken(key + "-session");
        Select select = QueryBuilder.select()
                .all()
                .from("pathstore_edgestream", tableName);
        select.where(QueryBuilder.eq("partition_key", key));
        for (Row row : globalSession.execute(select, sessionToken)) {
            stateVal = row.getString("state_val");
        }
        return stateVal;
    }

    public static void updateState(String key, String value) {
        SessionToken sessionToken = PathStoreSessionManager.getInstance().getKeyspaceToken(key + "-session");
        Update update = QueryBuilder.update("pathstore_edgestream", "opstatetable");
        update.where(QueryBuilder.eq("partition_key", key))
                .with(QueryBuilder.set("state_val", value));
        globalSession.execute(update, sessionToken);
    }

    public static void insertState(String OperatorID, String key, String value) {
        SessionToken sessionToken = PathStoreSessionManager.getInstance().getKeyspaceToken(key + "-session");
        Insert insert = QueryBuilder.insertInto("pathstore_edgestream", "opstatetable")
                .value("op_id", OperatorID)
                .value("partition_key", key)
                .value("state_val", value);
        globalSession.execute(insert, sessionToken);
    }

    // This function is used by EventWindowManager exclusively since event windows need multiple tables
    public static void insertStateWithTableName(String OperatorID, String key, String value, String tableName) {
        SessionToken sessionToken = PathStoreSessionManager.getInstance().getKeyspaceToken(key + "-session");
        Insert insert = QueryBuilder.insertInto("pathstore_edgestream", tableName)
                .value("op_id", OperatorID)
                .value("partition_key", key)
                .value("state_val", value);
        globalSession.execute(insert, sessionToken);
    }

    public static boolean partitionKeyExists(String key) {
        SessionToken sessionToken = PathStoreSessionManager.getInstance().getKeyspaceToken(key + "-session");
        Select select = QueryBuilder.select()
                .all()
                .from("pathstore_edgestream", "opstatetable");
        select.where(QueryBuilder.eq("partition_key", key));
        //PathStoreResultSet resultSet = globalSession.execute(select, sessionToken);
        PathStoreResultSet resultSet = null;
        return !resultSet.empty();
    }
}
