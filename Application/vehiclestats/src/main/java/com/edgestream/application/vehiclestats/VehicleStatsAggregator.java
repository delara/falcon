package com.edgestream.application.vehiclestats;

import com.edgestream.application.vehiclestats.operators.*;
import com.edgestream.client.EdgeStreamApplicationClient;
import com.edgestream.client.EdgeStreamApplicationContext;
import com.edgestream.client.EdgeStreamClientException;
import com.edgestream.worker.metrics.metricscollector2.MetricsCollector3;
import com.edgestream.worker.operator.Operator;
import com.edgestream.worker.operator.OperatorID;

import java.util.ArrayList;

public class VehicleStatsAggregator {
    public static void main(String[] args) {
        EdgeStreamApplicationClient edgeStreamApplicationClient = new EdgeStreamApplicationClient(args);
        Operator operator = getOperatorToRun(edgeStreamApplicationClient.getOperator_ID_asString(), edgeStreamApplicationClient.getInputType(), edgeStreamApplicationClient.getRoutingKeys(), args[7], edgeStreamApplicationClient.getMetricsCollector());
        EdgeStreamApplicationContext edgeStreamApplicationContext = new EdgeStreamApplicationContext(edgeStreamApplicationClient.getEdgeStreamApplicationConfig(), operator, edgeStreamApplicationClient.getMetricsCollector());
        edgeStreamApplicationContext.startApplication();

    }

    static Operator getOperatorToRun(String operator_ID_asString, String inputType, ArrayList routingKeys, String metrics_broker_IP_address, MetricsCollector3 metricsCollector) {
        if (inputType.equalsIgnoreCase("V")) {
            return new StatsAggregator(new OperatorID(operator_ID_asString), inputType, routingKeys, metrics_broker_IP_address, metricsCollector);

        } else if (inputType.equalsIgnoreCase("D")) {

            return new CSVWriter(new OperatorID(operator_ID_asString), inputType, metricsCollector);

        } else {

            throw new EdgeStreamClientException("No operator matching the type: " + inputType, new Throwable());
        }

    }
}
