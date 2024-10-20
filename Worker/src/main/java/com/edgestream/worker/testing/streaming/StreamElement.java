package com.edgestream.worker.testing.streaming;

import com.edgestream.worker.testing.watermark.Watermark;

public abstract class StreamElement {

    /**
     * Checks whether this element is a starstream.testing.watermark.
     * @return True, if this element is a starstream.testing.watermark, false otherwise.
     */
    public final boolean isWatermark() {
        return getClass() == Watermark.class;
    }


    /**
     * Checks whether this element is a record.
     * @return True, if this element is a record, false otherwise.
     */
    public final boolean isRecord() {
        return getClass() == StreamRecord.class;
    }

    /**
     * Checks whether this element is a latency marker.
     * @return True, if this element is a latency marker, false otherwise.
     */
    public final boolean isLatencyMarker() {
        return getClass() == LatencyMarker.class;
    }

    /**
     * Casts this element into a StreamRecord.
     * @return This element as a stream record.
     * @throws java.lang.ClassCastException Thrown, if this element is actually not a stream record.
     */
    @SuppressWarnings("unchecked")
    public final <E> StreamRecord<E> asRecord() {
        return (StreamRecord<E>) this;
    }

    /**
     * Casts this element into a Watermark.java.
     * @return This element as a Watermark.java.
     * @throws java.lang.ClassCastException Thrown, if this element is actually not a Watermark.java.
     */
    public final Watermark asWatermark() {
        return (Watermark) this;
    }



    /**
     * Casts this element into a LatencyMarker.
     * @return This element as a LatencyMarker.
     * @throws java.lang.ClassCastException Thrown, if this element is actually not a LatencyMarker.
     */
    public final LatencyMarker asLatencyMarker() {
        return (LatencyMarker) this;
    }
}