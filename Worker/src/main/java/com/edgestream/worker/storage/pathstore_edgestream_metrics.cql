CREATE KEYSPACE pathstore_edgestream_metrics WITH replication = {'class' : 'SimpleStrategy', 'replication_factor' : 1 }  AND durable_writes = false;

CREATE TABLE pathstore_edgestream_metrics.applicationMetrics (
    timeStamp BIGINT PRIMARY KEY,
    node_id TEXT,
    avg_latency_in_ms DOUBLE,
    min_latency_in_ms DOUBLE,
    max_latency_in_ms DOUBLE,
    std_latency_in_ms DOUBLE,
    median_latency_in_ms DOUBLE,
    operator_id TEXT,
    sequence_id TEXT,
    throughput TEXT,
    topology_id TEXT,
    input_rate TEXT,
    input_msg_size TEXT,
    output_msg_size TEXT,
    processing_time_ns TEXT,
    batch_size TEXT,
    buffer_consumer_size TEXT,
    buffer_producer_size TEXT,
    origin_list TEXT
) WITH bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = 'table definitions'
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.0
    AND default_time_to_live = 0
    AND gc_grace_seconds = 604800
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 3600000
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';