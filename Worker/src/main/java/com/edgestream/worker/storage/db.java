package com.edgestream.worker.storage;

import java.sql.*;

public class db {
    static Connection conn = null;
    static Statement s;
    static String connectionURL;
    public static boolean checkIfTableExists () throws SQLException {
        boolean chk = true;
        boolean doCreate = false;
        try {
            s.execute("select * from OpStateTable");
        }  catch (SQLException sqle) {
            String theError = (sqle).getSQLState();
            //   System.out.println("  Utils GOT:  " + theError);
            /** If table exists will get -  WARNING 02000: No row was found **/
            if (theError.equals("42X05"))   // Table does not exist
            {  return false;
            }  else if (theError.equals("42X14") || theError.equals("42821"))  {
                System.out.println("checkIfTableExists: Incorrect table definition. Drop table OpStateTable and rerun this program");
                throw sqle;
            } else {
                System.out.println("checkIfTableExists: Unhandled SQLException" );
                throw sqle;
            }
        }
        //  System.out.println("Just got the warning - table exists OK ");
        return true;
    }

    public static boolean partitionKeyExists(String key) {
        try {
            s = conn.createStatement();
            ResultSet partitionKeyDetails;
            String getPartitionKeyString = "SELECT STATE_VAL FROM OpStateTable WHERE PARTITION_KEY='" + key+"'";
            partitionKeyDetails = s.executeQuery(getPartitionKeyString);
            //s.close();
            //conn.close();
            //s.close();
            //conn.close();
            return partitionKeyDetails.next() != false;
        } catch (Throwable e)  {
            /*       Catch all exceptions and pass them to
             *       the Throwable.printStackTrace method  */
            System.out.println(" . . . exception thrown while checking for partition key:");
            e.printStackTrace(System.out);
            return false;
        }
    }

    public static String readState(String key) {
        try {
            s = conn.createStatement();
            ResultSet stateDetails;
            String stateVal = null;
            String getStateString = "SELECT STATE_VAL FROM OpStateTable WHERE PARTITION_KEY='" + key+"'";
            stateDetails = s.executeQuery(getStateString);
            if (stateDetails.next()) {
                stateVal = stateDetails.getString("STATE_VAL");
            }
            return stateVal;
        } catch (Throwable e)  {
            /*       Catch all exceptions and pass them to
             *       the Throwable.printStackTrace method  */
            System.out.println(" . . . exception thrown while reading state:");
            e.printStackTrace(System.out);
            return null;
        }
    }

    public static StateObject.OperatorState readOperatorState(String OperatorId) {
        try {
            s = conn.createStatement();
            ResultSet stateDetails;
            StateObject.OperatorState operatorState = new StateObject.OperatorState();
            Long partitionKey;
            String stateVal;
            int i = 0;
            String getStateString = "SELECT PARTITION_KEY, STATE_VAL FROM OpStateTable WHERE OP_ID='" + OperatorId+"'";
            stateDetails = s.executeQuery(getStateString);
            while (stateDetails.next()) {
                partitionKey = Long.parseLong(stateDetails.getString("PARTITION_KEY"));
                stateVal = stateDetails.getString("STATE_VAL");
                operatorState.partitionKeys.add(i, partitionKey);
                operatorState.stateValues.add(i, stateVal);
                i++;
            }
            return operatorState;
        } catch (Throwable e)  {
            /*       Catch all exceptions and pass them to
             *       the Throwable.printStackTrace method  */
            System.out.println(" . . . exception thrown while reading operator state:");
            e.printStackTrace(System.out);
            return null;
        }
    }

    public static void updateState(String key, String value) {
        try {
            PreparedStatement psUpdate;
            String updateStateString = "UPDATE OPSTATETABLE SET STATE_VAL=? WHERE PARTITION_KEY=?";
            psUpdate = conn.prepareStatement(updateStateString);
            psUpdate.setString(1, value);
            psUpdate.setString(2, key);
            psUpdate.executeUpdate();
        } catch (Throwable e) {
            /*       Catch all exceptions and pass them to
             *       the Throwable.printStackTrace method  */
            System.out.println(" . . . exception thrown while updating state:");
        }
    }

    public static void deleteState(String key) {
        try {
            PreparedStatement psDelete;
            String deleteStateString = "DELETE FROM OPSTATETABLE WHERE PARTITION_KEY=?";
            psDelete = conn.prepareStatement(deleteStateString);
            psDelete.setString(1, key);
            psDelete.executeUpdate();
        } catch (Throwable e) {
            /*       Catch all exceptions and pass them to
             *       the Throwable.printStackTrace method  */
            System.out.println(" . . . exception thrown while deleting state:");
        }
    }

    public static void insertState(String OperatorID, String key, String value) {
        try {
            PreparedStatement psInsert;
            String insertNewStateString = "INSERT INTO OPSTATETABLE(OP_ID, PARTITION_KEY, STATE_VAL) values (?,?,?)";
            psInsert = conn.prepareStatement(insertNewStateString);
            psInsert.setString(1,OperatorID);
            psInsert.setString(2,key);
            psInsert.setString(3,value);
            psInsert.executeUpdate();
        } catch (Throwable e)  {
            /*       Catch all exceptions and pass them to
             *       the Throwable.printStackTrace method  */
            System.out.println(" . . . exception thrown while inserting state:");
            e.printStackTrace(System.out);
        }
    }

    public static void initialiseDBConnection() {
        String driver = "org.apache.derby.jdbc.ClientDriver";
        String dbName="stateDB";
        connectionURL = "jdbc:derby://localhost:1527/" + dbName + ";create=true";
        try {
            conn = DriverManager.getConnection(connectionURL);
            System.out.println("Connected to database " + dbName);
        } catch (Throwable e)  {
            /*       Catch all exceptions and pass them to
             *       the Throwable.printStackTrace method  */
            System.out.println(" . . . exception thrown:");
            e.printStackTrace(System.out);
        }
    }

    public static void initialiseDB() {
        String driver = "org.apache.derby.jdbc.ClientDriver";
        String dbName="stateDB";
        connectionURL = "jdbc:derby://localhost:1527/" + dbName + ";create=true";
        PreparedStatement deleteSt;

        String createString = "CREATE TABLE OpStateTable  "
                +  "(OP_ID VARCHAR(64) NOT NULL, "
                +  " ENTRY_DATE TIMESTAMP DEFAULT CURRENT_TIMESTAMP, "
                +  " PARTITION_KEY VARCHAR(64) NOT NULL "
                +  "   CONSTRAINT OP_PK PRIMARY KEY, "
                +  " STATE_VAL VARCHAR(512) NOT NULL) ";

        String deleteString = "DELETE FROM OpStateTable";

        try {
            conn = DriverManager.getConnection(connectionURL);
            System.out.println("Connected to database " + dbName);
            //System.out.println(conn.getClientInfo());
            s = conn.createStatement();
            if (! checkIfTableExists()) {
                System.out.println (" . . . . creating table OpStateTable");
                s.execute(createString);
            } else {
                System.out.println(". . . . cleaning table OpStateTable");
                deleteSt = conn.prepareStatement(deleteString);
                deleteSt.executeUpdate();
            }

            //   ## DATABASE SHUTDOWN SECTION ##
            /*** In embedded mode, an application should shut down Derby.
             Shutdown throws the XJ015 exception to confirm success. ***/
            if (driver.equals("org.apache.derby.jdbc.EmbeddedDriver")) {
                boolean gotSQLExc = false;
                try {
                    DriverManager.getConnection("jdbc:derby:;shutdown=true");
                } catch (SQLException se)  {
                    if ( se.getSQLState().equals("XJ015") ) {
                        gotSQLExc = true;
                    }
                }
                if (!gotSQLExc) {
                    System.out.println("Database did not shut down normally");
                }  else  {
                    System.out.println("Database shut down normally");
                }
            }
        } catch (Throwable e)  {
            /*       Catch all exceptions and pass them to
             *       the Throwable.printStackTrace method  */
            System.out.println(" . . . exception thrown:");
            e.printStackTrace(System.out);
        }
    }
}
