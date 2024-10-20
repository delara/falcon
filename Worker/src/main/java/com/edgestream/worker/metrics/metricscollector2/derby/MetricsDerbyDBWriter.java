package com.edgestream.worker.metrics.metricscollector2.derby;

import com.edgestream.worker.metrics.common.InputEventMetric;
import com.edgestream.worker.metrics.common.OutputEventMetric;
import com.edgestream.worker.metrics.metricscollector2.MetricsCollectorDBWriter;

import java.sql.Connection;
import java.sql.SQLException;

public class MetricsDerbyDBWriter implements MetricsCollectorDBWriter {

    private final Connection conn;

    public MetricsDerbyDBWriter(Connection conn) {
        this.conn = conn;
    }

    @Override
    public void addInputEvent(InputEventMetric inputEventMetric) {
        String INPUT_EventSQLStatement = null; //TODO: implement SQL insert to table here
        try {
            this.conn.prepareStatement(INPUT_EventSQLStatement);
        }catch(SQLException throwables){
            throwables.printStackTrace();
        }
    }

    @Override
    public void addOutputEvent(OutputEventMetric outputEventMetric) {
        String OUTPUT_EventSQLStatement = null; //TODO: implement SQL insert to table here
        try {
            this.conn.prepareStatement(OUTPUT_EventSQLStatement);
        }catch(SQLException throwables){
            throwables.printStackTrace();
        }
    }
}
