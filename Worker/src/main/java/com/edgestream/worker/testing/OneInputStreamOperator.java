package com.edgestream.worker.testing;

import com.edgestream.worker.testing.streaming.LatencyMarker;
import com.edgestream.worker.testing.streaming.StreamRecord;
import com.edgestream.worker.testing.watermark.Watermark;

public interface OneInputStreamOperator<IN, OUT> extends StreamOperator<OUT> {

    /**
     * Processes one element that arrived at this starstream.operator.
     * This method is guaranteed to not be called concurrently with other methods of the starstream.operator.
     */
    void processElement(StreamRecord<IN> element) throws Exception;

    /**
     * Processes a {@link Watermark}.
     * This method is guaranteed to not be called concurrently with other methods of the starstream.operator.
     *
     * @see org.apache.flink.streaming.api.watermark.Watermark
     */
    void processWatermark(Watermark mark) throws Exception;

    void processLatencyMarker(LatencyMarker latencyMarker) throws Exception;
}
