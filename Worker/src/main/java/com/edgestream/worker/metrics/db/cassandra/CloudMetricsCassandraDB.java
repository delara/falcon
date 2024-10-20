package com.edgestream.worker.metrics.db.cassandra;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.querybuilder.schema.CreateKeyspace;
import com.datastax.oss.driver.api.querybuilder.schema.CreateTable;
import com.datastax.oss.driver.api.querybuilder.schema.Drop;

import static com.datastax.oss.driver.api.querybuilder.SchemaBuilder.*;
import java.net.InetSocketAddress;


public class CloudMetricsCassandraDB implements MetricsCassandraDBService {

    private final CqlSession session;

    public CloudMetricsCassandraDB() {
        session = CqlSession.builder()
                .addContactPoint(new InetSocketAddress("10.70.2.12", 9042))
                .withLocalDatacenter("datacenter1")
                .build();

        CreateKeyspace createKs = createKeyspace("metrics_log").ifNotExists().withSimpleStrategy(1);
        session.execute(createKs.build());
        // This is not thread-safe
        // session.execute("USE metrics_log");
        createApplicationMetricsSchema();
    }

    public CqlSession getDatabaseSession() {
        return this.session;
    }

    private void createApplicationMetricsSchema() {
        // TODO: create AplicationMetrics table in the keyspace
        // Drop old table if exists
        Drop dropTable = dropTable("metrics_log", "applicationMetrics").ifExists();
        session.execute(dropTable.build());

        // Create new table
        CreateTable createTable = createTable("metrics_log", "applicationMetrics")
                .withPartitionKey("timeStamp", DataTypes.BIGINT)
                .withColumn("node_id", DataTypes.TEXT)
                .withColumn("avg_latency_in_ms", DataTypes.DOUBLE)
                .withColumn("min_latency_in_ms", DataTypes.TEXT)
                .withColumn("max_latency_in_ms", DataTypes.TEXT)
                .withColumn("std_latency_in_ms", DataTypes.TEXT)
                .withColumn("median_latency_in_ms", DataTypes.TEXT)
                .withColumn("operator_id", DataTypes.TEXT)
                .withColumn("sequence_id", DataTypes.TEXT)
                .withColumn("throughput", DataTypes.TEXT)
                .withColumn("topology_id", DataTypes.TEXT)
                .withColumn("input_rate", DataTypes.TEXT)
                .withColumn("input_msg_size", DataTypes.TEXT)
                .withColumn("output_msg_size", DataTypes.TEXT)
                .withColumn("processing_time_ns", DataTypes.TEXT)
                .withColumn("batch_size", DataTypes.TEXT)
                .withColumn("buffer_consumer_size", DataTypes.TEXT)
                .withColumn("buffer_producer_size", DataTypes.TEXT)
                .withColumn("origin_list", DataTypes.TEXT);
        session.execute(createTable.build());
    }


}
