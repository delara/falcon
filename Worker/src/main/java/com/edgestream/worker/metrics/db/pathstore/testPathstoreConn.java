package com.edgestream.worker.metrics.db.pathstore;

import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Truncate;
import com.edgestream.worker.metrics.model.ApplicationMetric;
import com.edgestream.worker.storage.PathstoreClient;
import pathstore.client.PathStoreClientAuthenticatedCluster;
import pathstore.client.PathStoreSession;
import pathstore.common.PathStoreProperties;
import pathstore.sessions.PathStoreSessionManager;
import pathstore.sessions.SessionToken;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.literal;

public class testPathstoreConn {

    private static final String METRICS_KEYSPACE = "pathstore_edgestream_metrics";
    private static PathStoreSession session;

    private static void wipeOutMetricsTable(String table) {
        Truncate truncate = QueryBuilder.truncate(METRICS_KEYSPACE, table);
        SessionToken sessionToken = PathStoreSessionManager.getInstance().getKeyspaceToken( METRICS_KEYSPACE + "-" + table + "-session");
        session.execute(truncate, sessionToken);
    }

    public static void main(String[] args) {
        PathStoreProperties.PROPERTIES_LOCATION = "C:\\Users\\jason\\IdeaProjects\\pathstore.properties";
        PathstoreClient.initialiseConnection();
        session = PathStoreClientAuthenticatedCluster.getInstance().pathStoreSession();
        wipeOutMetricsTable("applicationmetrics");
        Insert insert = QueryBuilder.insertInto(METRICS_KEYSPACE, "applicationmetrics")
                .value("timeStamp", Long.parseLong("1234567"))
                .value("node_id", "33")
                .value("avg_latency_in_ms", Double.parseDouble("4353463"))
                .value("min_latency_in_ms", Double.parseDouble("623423"))
                .value("max_latency_in_ms", Double.parseDouble("752342345"))
                .value("std_latency_in_ms", Double.parseDouble("923423423423"))
                .value("median_latency_in_ms", Double.parseDouble("512354647"));

//                .value("operator_id", literal(applicationMetric.getOperator_id()))
//                .value("sequence_id", literal(applicationMetric.getSequence_id()))
//                .value("throughput", literal(applicationMetric.getThroughput()))
//                .value("topology_id", literal(applicationMetric.getTopology_id()))
//                .value("input_rate", literal(applicationMetric.getInput_rate()))
//                .value("input_msg_size", literal(applicationMetric.getInput_msg_size()))
//                .value("output_msg_size", literal(applicationMetric.getOutput_msg_size()))
//                .value("processing_time_ns", literal(applicationMetric.getProcessing_time_ns()))
//                .value("batch_size", literal(applicationMetric.getBatch_size()))
//                .value("buffer_consumer_size", literal(applicationMetric.getBuffer_consumer_size()))
//                .value("buffer_producer_size", literal(applicationMetric.getBuffer_producer_size()))
//                .value("origin_list", literal(applicationMetric.getTupleOriginListAsString()));
        SessionToken token = PathStoreSessionManager.getInstance().getKeyspaceToken(METRICS_KEYSPACE + "-applicationmetrics-session");
        session.execute(insert, token);
    }

}
