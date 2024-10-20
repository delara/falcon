package com.edgestream.worker.metrics.metricscollector2;

public interface MetricsCollectorReader {

    void getInputEvents();
    void getOutputEvents();

}
