package com.edgestream.worker.metrics.metricscollector2;

import com.edgestream.worker.metrics.common.InputEventMetric;
import com.edgestream.worker.metrics.common.OutputEventMetric;

public interface MetricsCollectorDBWriter {

    void addInputEvent(InputEventMetric inputEventMetric);

    void addOutputEvent(OutputEventMetric outputEventMetric);

}
