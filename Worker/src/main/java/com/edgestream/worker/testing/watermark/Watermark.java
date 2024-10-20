package com.edgestream.worker.testing.watermark;

import com.edgestream.worker.testing.streaming.StreamElement;

public final class Watermark extends StreamElement {

    /** The starstream.testing.watermark that signifies end-of-event-time. */
    public static final Watermark MAX_WATERMARK = new Watermark(Long.MAX_VALUE);

    // ------------------------------------------------------------------------

    /** The timestamp of the starstream.testing.watermark in milliseconds. */
    private final long timestamp;

    /**
     * Creates a new starstream.testing.watermark with the given timestamp in milliseconds.
     */
    public Watermark(long timestamp) {
        this.timestamp = timestamp;
    }

    /**
     * Returns the timestamp associated with this {@link Watermark} in milliseconds.
     */
    public long getTimestamp() {
        return timestamp;
    }

    // ------------------------------------------------------------------------

    @Override
    public boolean equals(Object o) {
        return this == o ||
                o != null && o.getClass() == Watermark.class && ((Watermark) o).timestamp == this.timestamp;
    }

    @Override
    public int hashCode() {
        return (int) (timestamp ^ (timestamp >>> 32));
    }

    @Override
    public String toString() {
        return "Watermark @ " + timestamp;
    }
}