package com.edgestream.worker.metrics.metricscollector2.derby;

import com.edgestream.worker.metrics.common.InputEventMetric;
import com.edgestream.worker.metrics.common.OutputEventMetric;
import com.edgestream.worker.metrics.metricscollector2.ContainerMetricsDB;
import org.apache.derby.jdbc.BasicEmbeddedDataSource40;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

public class ContainerMetricsDerbyDB implements ContainerMetricsDB {

    private Connection conn;
    private final MetricsDerbyDBWriter metricsDerbyDBWriter;

    public ContainerMetricsDerbyDB() {
        //1. Create new derby instance
        DataSource ds = makeDataSource("ContainerMetricsDB",true);
        try {
            this.conn = ds.getConnection();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }

        metricsDerbyDBWriter = new MetricsDerbyDBWriter(conn);

        //2. Create schemas
        createInputEventSchema();
    }


    /****************************************************************************************
     *
     *      CRUD operations
     *
     ***************************************************************************************/

    @Override
    public void insertInputEvent(InputEventMetric inputEventMetric) {
        metricsDerbyDBWriter.addInputEvent(inputEventMetric);
    }

    @Override
    public void insertOutputEvent(OutputEventMetric outputEventMetric) {
        metricsDerbyDBWriter.addOutputEvent(outputEventMetric);
    }


    @Override
    public ArrayList<InputEventMetric> queryNextInputEvents() {
        //TODO: make a metricsDerbyDBReader
        return null;
    }

    @Override
    public ArrayList<OutputEventMetric> queryNextOutputEvents() {
        //TODO: make a metricsDerbyDBReader
        return null;
    }



    private DataSource makeDataSource (String dbname, boolean create){
        BasicEmbeddedDataSource40 ds = new BasicEmbeddedDataSource40();
        ds.setDatabaseName(dbname);
        if (create) {
            ds.setCreateDatabase("create");
        }
        return ds;
    }


    /**********************************************************************************
     *
     *
     *                                 Create Schemas
     *
     **********************************************************************************/


    private void createInputEventSchema() {
        Statement stmt = null;
        try {
            stmt = this.conn.createStatement();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }

        try {
            String dropQuery = "DROP TABLE inputEvents";
            stmt.execute(dropQuery);
        }catch (Exception e){
            System.out.println("[inputEvents] Table does not exist");
        }

        String query1 = "CREATE TABLE inputEvents("
                + "timeStamp VARCHAR(255),"
                + "sequence_id VARCHAR(255),"
                + "message_type VARCHAR(255),"
                + "topology_id VARCHAR(255),"
                + "node_id VARCHAR(255),"
                + "operator_id VARCHAR(255),"
                + "tupleType VARCHAR(255),"
                + "batch_size VARCHAR(255),"
                + "buffer_consumer_size VARCHAR(255),"
                + "buffer_producer_size VARCHAR(255),"
                + "state_collection BLOB" +
                ")";


        try {
            stmt.execute(query1);
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }

    }



}
