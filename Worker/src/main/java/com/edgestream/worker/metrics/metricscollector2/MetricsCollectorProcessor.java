package com.edgestream.worker.metrics.metricscollector2;


import com.edgestream.worker.metrics.metricscollector2.derby.ContainerMetricsDerbyDB;

public class MetricsCollectorProcessor {

    MetricsProcessor metricsProcessor;

    public MetricsCollectorProcessor(MetricsProcessor metricsProcessor, ContainerMetricsDerbyDB containerMetricsDerbyDB) {
        this.metricsProcessor = metricsProcessor;


    }



}
