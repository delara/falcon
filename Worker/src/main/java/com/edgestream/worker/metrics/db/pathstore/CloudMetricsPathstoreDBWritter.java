package com.edgestream.worker.metrics.db.pathstore;

import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.edgestream.worker.metrics.model.ApplicationMetric;
import pathstore.sessions.PathStoreSessionManager;
import pathstore.sessions.SessionToken;


public class CloudMetricsPathstoreDBWritter {

    private final CloudMetricsPathstoreDB pathstoreDB;

    public CloudMetricsPathstoreDBWritter (CloudMetricsPathstoreDB cloudMetricsPathstoreDB) {
        pathstoreDB = cloudMetricsPathstoreDB;
    }

    public void insertApplicationMetric(ApplicationMetric applicationMetric) {
        Insert insert = QueryBuilder.insertInto("pathstore_edgestream_metrics", "applicationmetrics")
                .value("timeStamp", Long.parseLong(applicationMetric.getTimeStamp()))
                .value("node_id", applicationMetric.getNode_id())
                .value("avg_latency_in_ms", Double.parseDouble(applicationMetric.getAvgLatency_in_ms()))
                .value("min_latency_in_ms", Double.parseDouble(applicationMetric.getMinLatency_in_ms()))
                .value("max_latency_in_ms", Double.parseDouble(applicationMetric.getMaxLatency_in_ms()))
                .value("std_latency_in_ms", Double.parseDouble(applicationMetric.getStdLatency_in_ms()))
                .value("median_latency_in_ms", Double.parseDouble(applicationMetric.getMedianLatency_in_ms()))
                .value("operator_id", applicationMetric.getOperator_id())
                .value("sequence_id", applicationMetric.getSequence_id())
                .value("throughput", applicationMetric.getThroughput())
                .value("topology_id", applicationMetric.getTopology_id())
                .value("input_rate", applicationMetric.getInput_rate())
                .value("input_msg_size", applicationMetric.getInput_msg_size())
                .value("output_msg_size", applicationMetric.getOutput_msg_size())
                .value("processing_time_ns", applicationMetric.getProcessing_time_ns())
                .value("batch_size", applicationMetric.getBatch_size())
                .value("buffer_consumer_size", applicationMetric.getBuffer_consumer_size())
                .value("buffer_producer_size", applicationMetric.getBuffer_producer_size())
                .value("origin_list", applicationMetric.getTupleOriginListAsString());

        SessionToken token = PathStoreSessionManager.getInstance().getKeyspaceToken("metrics-session");
        pathstoreDB.getDatabaseSession().execute(insert, token);
    }
}
