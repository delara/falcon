package com.edgestream.worker.metrics.cloudDB;

import java.sql.Connection;

public interface MetricsDBService {

    Connection getDatabaseConnection();

}
