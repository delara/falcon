package com.edgestream.worker.metrics.db.cassandra;

import com.datastax.oss.driver.api.querybuilder.insert.Insert;
import com.datastax.oss.driver.api.querybuilder.insert.InsertInto;
import com.edgestream.worker.metrics.model.ApplicationMetric;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.*;

public class CloudMetricsCassandraDBWritter {

    private final MetricsCassandraDBService metricsCassandraDBService;

    public CloudMetricsCassandraDBWritter(MetricsCassandraDBService metricsCassandraDBService) {
        this.metricsCassandraDBService = metricsCassandraDBService;
    }

    public void putApplicationMetric(ApplicationMetric applicationMetric) {
        // TODO: Implement
        Insert insertData = insertInto("metrics_log", "applicationMetrics")
                .value("timeStamp", literal(Long.parseLong(applicationMetric.getTimeStamp())))
                .value("node_id", literal(applicationMetric.getNode_id()))
                .value("avg_latency_in_ms", literal(Double.parseDouble(applicationMetric.getAvgLatency_in_ms())))
                .value("min_latency_in_ms", literal(applicationMetric.getMinLatency_in_ms()))
                .value("max_latency_in_ms", literal(applicationMetric.getMaxLatency_in_ms()))
                .value("std_latency_in_ms", literal(applicationMetric.getStdLatency_in_ms()))
                .value("median_latency_in_ms", literal(applicationMetric.getMedianLatency_in_ms()))
                .value("operator_id", literal(applicationMetric.getOperator_id()))
                .value("sequence_id", literal(applicationMetric.getSequence_id()))
                .value("throughput", literal(applicationMetric.getThroughput()))
                .value("topology_id", literal(applicationMetric.getTopology_id()))
                .value("input_rate", literal(applicationMetric.getInput_rate()))
                .value("input_msg_size", literal(applicationMetric.getInput_msg_size()))
                .value("output_msg_size", literal(applicationMetric.getOutput_msg_size()))
                .value("processing_time_ns", literal(applicationMetric.getProcessing_time_ns()))
                .value("batch_size", literal(applicationMetric.getBatch_size()))
                .value("buffer_consumer_size", literal(applicationMetric.getBuffer_consumer_size()))
                .value("buffer_producer_size", literal(applicationMetric.getBuffer_producer_size()))
                .value("origin_list", literal(applicationMetric.getTupleOriginListAsString()));
        metricsCassandraDBService.getDatabaseSession().execute(insertData.build());
    }
}
