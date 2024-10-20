package com.edgestream.worker.metrics.db.cassandra;

import com.datastax.oss.driver.api.core.CqlSession;

public interface MetricsCassandraDBService {
    CqlSession getDatabaseSession();

}
