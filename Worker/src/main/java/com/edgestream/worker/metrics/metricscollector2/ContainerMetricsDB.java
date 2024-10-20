package com.edgestream.worker.metrics.metricscollector2;

import com.edgestream.worker.metrics.common.InputEventMetric;
import com.edgestream.worker.metrics.common.OutputEventMetric;

import java.util.ArrayList;

public interface ContainerMetricsDB {

    void insertInputEvent(InputEventMetric inputEventMetric);
    void insertOutputEvent(OutputEventMetric outputEventMetric);

    ArrayList<InputEventMetric> queryNextInputEvents();
    ArrayList<OutputEventMetric> queryNextOutputEvents();
}
